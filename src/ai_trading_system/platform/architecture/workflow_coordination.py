"""DEVX-015 execution lifecycle in the existing lease event authority.

No second store, lock, execution queue or scheduler. A caller must hold its
ordinary task/validation authority before reserving; this module additionally
keeps execution ownership until actual contained-process termination is known.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

from ai_trading_system.platform.architecture.parallel_control import ParallelControlError

if TYPE_CHECKING:
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
        _RemotePreparation,
    )
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        ExecutionLease,
        FileExecutionLeaseStore,
    )
    from ai_trading_system.platform.architecture.workflow_contract import (
        _BoundDirectoryCustody,
        _BoundReadFileCustody,
    )
    from ai_trading_system.platform.architecture.workflow_execution import (
        InheritedJobChild,
        WindowsJobProcess,
    )

# Hook-input custody of the Full profile inspection captures. The captured runtime profile grows with the
# node count (about 1.9 KB per node, 27.4 MB for the v18 Full), so it exceeds the generic 16 MiB artifact
# default; DEVX-018 v19. This is an engineering ceiling, not a model rule: the reader's 64 MiB hard cap
# applies and larger evidence fails closed instead of being truncated or skipped.
PUBLICATION_CAPTURE_BUDGET_BYTES = 64 * 1024 * 1024

_REQUEST_KEYS = {
    "schema_version",
    "request_id",
    "lease_id",
    "manifest_sha256",
    "candidate_sha",
    "validation_identity_sha256",
    "argv",
    "cwd",
    "environment_sha256",
    "stdout_path",
    "result_path",
    "job_name",
    "host_id",
    "writer_epoch",
    "subject_task_id",
    "task_authority_sha256",
}
_CHECKPOINT_REQUEST_KEYS = (
    _REQUEST_KEYS - {"candidate_sha", "validation_identity_sha256", "task_authority_sha256"}
) | {
    "execution_kind",
    "source_root",
    "source_head_sha",
    "checkpoint_id",
    "checkpoint_task_id",
    "checkpoint_thread_id",
    "checkpoint_request_sha256",
    "checkpoint_intent_id",
    "checkpoint_intent_sha256",
    "scope_intent_sha256",
}
_SOURCE_CANDIDATE_REQUEST_KEYS = (
    _REQUEST_KEYS - {"candidate_sha", "validation_identity_sha256"}
) | {
    "execution_kind",
    "source_head_sha",
    "source_request_sha256",
    "source_transaction_sha256",
    "review_sha256",
}
_INSTALLATION_REQUEST_KEYS = _SOURCE_CANDIDATE_REQUEST_KEYS | {
    "installed_candidate_sha",
    "source_result_sha256",
    "installation_plan_sha256",
    "installation_action",
    "installation_attempt",
    "previous_installation_sha256",
}
_PUBLICATION_REQUEST_KEYS = (_REQUEST_KEYS - {"validation_identity_sha256"}) | {
    "execution_kind",
    "publication_transaction_path",
    "publication_transaction_sha256",
    "local_publication_event_id",
    "local_publication_intent_sha256",
    "full_execution_sha256",
    "expected_main_sha",
    "publication_action",
    "publication_attempt",
    "previous_publication_sha256",
}
_STATES = {
    "RESERVED": {"CONTAINED_SUSPENDED", "EXIT_CONFIRMED"},
    "CONTAINED_SUSPENDED": {"RESUME_INTENT", "EXIT_CONFIRMED"},
    "RESUME_INTENT": {"RUNNING", "EXIT_CONFIRMED"},
    "RUNNING": {"EXIT_CONFIRMED"},
    "EXIT_CONFIRMED": {"RESULT_RECORDED"},
    "RESULT_RECORDED": set(),
}


def _fail(code: str) -> NoReturn:
    raise ParallelControlError("LEASE_EXECUTION_" + code, "execution lifecycle rejected")


def _copy(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))


def _digest(value: object, size: int = 64) -> None:
    if not isinstance(value, str) or re.fullmatch("[a-f0-9]{" + str(size) + "}", value) is None:
        _fail("IDENTITY")


def _request(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        _fail("REQUEST_FIELDS")
    checkpoint = value.get("schema_version") == "workflow_execution_request.v2"
    source_candidate = value.get("schema_version") == "workflow_execution_request.v3"
    installation = value.get("schema_version") == "workflow_execution_request.v4"
    publication = value.get("schema_version") == "workflow_execution_request.v5"
    keys = (
        _PUBLICATION_REQUEST_KEYS
        if publication
        else _INSTALLATION_REQUEST_KEYS
        if installation
        else _SOURCE_CANDIDATE_REQUEST_KEYS
        if source_candidate
        else _CHECKPOINT_REQUEST_KEYS
        if checkpoint
        else _REQUEST_KEYS
    )
    if set(value) != keys:
        _fail("REQUEST_FIELDS")
    request = _copy(dict(value))
    if request["schema_version"] not in {
        "workflow_execution_request.v1",
        "workflow_execution_request.v2",
        "workflow_execution_request.v3",
        "workflow_execution_request.v4",
        "workflow_execution_request.v5",
    }:
        _fail("REQUEST_SCHEMA")
    digests = ["manifest_sha256", "environment_sha256"]
    if checkpoint:
        if request["execution_kind"] != "TASK_SOURCE_CAPTURE":
            _fail("REQUEST_KIND")
        digests += ["checkpoint_request_sha256", "checkpoint_intent_sha256", "scope_intent_sha256"]
        _digest(request["source_head_sha"], 40)
        if (
            not isinstance(request["checkpoint_id"], str)
            or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,95}", request["checkpoint_id"]) is None
            or not isinstance(request["checkpoint_intent_id"], str)
            # Historical UUID intents remain readable. New attempt v2 hashes
            # bind producer/request to the real immutable lease change_id;
            # checkpoint admission independently verifies that full binding.
            or re.fullmatch(
                r"task-checkpoint-(?:[a-f0-9]{32}|[a-f0-9]{64})",
                request["checkpoint_intent_id"],
            )
            is None
        ):
            _fail("CHECKPOINT_IDENTITY")
        for key in ("checkpoint_task_id", "checkpoint_thread_id"):
            if not isinstance(request[key], str) or not request[key] or "\0" in request[key]:
                _fail("CHECKPOINT_IDENTITY")
        if (
            not isinstance(request["source_root"], str)
            or not Path(request["source_root"]).is_absolute()
        ):
            _fail("PATH")
    elif source_candidate or installation:
        kind = "CONTROLLED_SOURCE_INSTALLATION" if installation else "CONTROLLED_SOURCE_CANDIDATE"
        if request["execution_kind"] != kind:
            _fail("REQUEST_KIND")
        _digest(request["source_head_sha"], 40)
        digests += [
            "source_request_sha256",
            "source_transaction_sha256",
            "task_authority_sha256",
            "review_sha256",
        ]
        if installation:
            _digest(request["installed_candidate_sha"], 40)
            digests += ["source_result_sha256", "installation_plan_sha256"]
            attempt = request["installation_attempt"]
            if type(attempt) is not int or attempt < 1:
                _fail("INSTALLATION_ATTEMPT")
            if attempt == 1:
                if (
                    request["installation_action"] != "INSTALL"
                    or request["previous_installation_sha256"] is not None
                ):
                    _fail("INSTALLATION_ATTEMPT")
            else:
                if request["installation_action"] != "RECOVER":
                    _fail("INSTALLATION_ATTEMPT")
                digests.append("previous_installation_sha256")
    elif publication:
        if request["execution_kind"] != "CONTROLLED_LOCAL_PUBLICATION":
            _fail("REQUEST_KIND")
        _digest(request["candidate_sha"], 40)
        _digest(request["expected_main_sha"], 40)
        digests += [
            "task_authority_sha256", "publication_transaction_sha256",
            "local_publication_event_id", "local_publication_intent_sha256",
            "full_execution_sha256",
        ]
        transaction = request["publication_transaction_path"]
        if (
            not isinstance(transaction, str)
            or not Path(transaction).is_absolute()
            or ".." in Path(transaction).parts
            or not isinstance(request["cwd"], str)
            or not Path(transaction).is_relative_to(Path(request["cwd"]))
        ):
            _fail("PUBLICATION_TRANSACTION_PATH")
        attempt = request["publication_attempt"]
        if type(attempt) is not int or attempt < 1:
            _fail("PUBLICATION_ATTEMPT")
        if attempt == 1:
            if (
                request["publication_action"] != "PUBLISH"
                or request["previous_publication_sha256"] is not None
            ):
                _fail("PUBLICATION_ATTEMPT")
        else:
            if request["publication_action"] != "RECOVER":
                _fail("PUBLICATION_ATTEMPT")
            digests.append("previous_publication_sha256")
    else:
        digests += ["validation_identity_sha256", "task_authority_sha256"]
        _digest(request["candidate_sha"], 40)
    for key in digests:
        _digest(request[key])
    for key in ("request_id", "lease_id", "host_id", "writer_epoch", "subject_task_id"):
        if not isinstance(request[key], str) or not request[key] or "\0" in request[key]:
            _fail("REQUEST_IDENTITY")
    if (
        not isinstance(request["argv"], list)
        or not request["argv"]
        or any(not isinstance(arg, str) or "\0" in arg for arg in request["argv"])
    ):
        _fail("ARGV")
    for path in (
        request["argv"][0],
        request["cwd"],
        request["stdout_path"],
        request["result_path"],
    ):
        if not isinstance(path, str) or not Path(path).is_absolute():
            _fail("PATH")
    if (
        not isinstance(request["job_name"], str)
        or re.fullmatch(r"Local\\AITS-DEVX015-[A-Za-z0-9-]{8,96}", request["job_name"]) is None
    ):
        _fail("JOB_NAME")
    return dict(request)


def _result_binding(request: Mapping[str, Any]) -> dict[str, Any]:
    if request["schema_version"] == "workflow_execution_request.v5":
        keys = (_PUBLICATION_REQUEST_KEYS - _REQUEST_KEYS) | {
            "request_id", "lease_id", "candidate_sha", "manifest_sha256",
        }
        return {
            **{key: request[key] for key in keys},
            "schema_version": "controlled_local_publication_worker_result.v1",
        }
    if request["schema_version"] == "workflow_execution_request.v4":
        keys = (_INSTALLATION_REQUEST_KEYS - _REQUEST_KEYS) | {"request_id"}
        return {
            **{key: request[key] for key in keys},
            "schema_version": "controlled_source_installation_worker_result.v1",
        }
    if request["schema_version"] == "workflow_execution_request.v3":
        return {
            "schema_version": "controlled_source_candidate_worker_result.v1",
            **{
                key: request[key]
                for key in (
                    "request_id",
                    "execution_kind",
                    "source_head_sha",
                    "source_request_sha256",
                    "source_transaction_sha256",
                    "review_sha256",
                )
            },
        }
    if request["schema_version"] == "workflow_execution_request.v2":
        return {
            "schema_version": "task_checkpoint_worker_result.v1",
            **{
                key: request[key]
                for key in (
                    "request_id",
                    "execution_kind",
                    "source_head_sha",
                    "checkpoint_id",
                    "checkpoint_task_id",
                    "checkpoint_request_sha256",
                    "checkpoint_intent_sha256",
                )
            },
        }
    return {
        key: request[key] for key in ("candidate_sha", "validation_identity_sha256", "request_id")
    }


def _process(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {"pid", "creation_time"}:
        _fail("PROCESS_IDENTITY")
    if any(type(value[key]) is not int or value[key] <= 0 for key in value):
        _fail("PROCESS_IDENTITY")


def _full_record(record: object, request: Mapping[str, Any], exit_fact: Mapping[str, Any]) -> None:
    if not isinstance(record, dict) or set(record) != {
        "schema_version",
        "candidate_sha",
        "validation_identity_sha256",
        "request_id",
        "status",
        "summary",
    }:
        _fail("FULL_COMMITMENT_BINDING")
    expected = {"schema_version": "full_execution_result.v1", **_result_binding(request)}
    if any(record.get(key) != item for key, item in expected.items()) or (
        record["status"] not in {"PASS", "FAIL", "INSUFFICIENT", "INVALID"}
        or (record["status"] == "PASS" and exit_fact.get("returncode") != 0)
        or exit_fact.get("basis") != "LIVE_CONTAINED_HANDLE"
    ):
        _fail("FULL_COMMITMENT_BINDING")
    summary = record["summary"]
    if not isinstance(summary, dict) or set(summary) != {"path", "sha256"}:
        _fail("FULL_COMMITMENT_SUMMARY")
    if (
        summary["path"]
        != (Path(request["result_path"]).parent / "test_runtime_summary.json").absolute().as_posix()
    ):
        _fail("FULL_COMMITMENT_SUMMARY")
    _digest(summary["sha256"])


def full_execution_projection(execution: Mapping[str, Any]) -> dict[str, Any]:
    """Recover the immutable original Full record, never a new Full result."""
    value = dict(execution)
    if value.get("schema_version") == "lease_execution.v5":
        value.pop("publication_attempts", None)
        value.pop("publication_stable_observation", None)
        value["schema_version"] = "lease_execution.v4"
    # Deep-copy only the retained original Full, not the large publication
    # readsets that this projection intentionally discards.
    return dict(_copy(value))


def _require_index_replaced_topology(
    original: Mapping[str, Any], observed: Mapping[str, Any],
) -> None:
    """Permit only a replaced candidate index, never other topology drift."""
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256

    for topology in (original, observed):
        if not isinstance(topology, Mapping):
            _fail("PUBLICATION_INDEX_RECOVERY_TOPOLOGY")
        checkout = topology.get("candidate_checkout")
        if not isinstance(checkout, Mapping):
            _fail("PUBLICATION_INDEX_RECOVERY_TOPOLOGY")
        index = checkout.get("index")
        if not isinstance(index, Mapping) or set(index) != {"path", "identity", "sha256", "size"}:
            _fail("PUBLICATION_INDEX_RECOVERY_TOPOLOGY")
        identity = index["identity"]
        if (not isinstance(index["path"], str) or not index["path"]
                or not isinstance(identity, list) or len(identity) != 2
                or any(type(item) is not int or item < 0 for item in identity)
                or type(index["size"]) is not int or index["size"] <= 0):
            _fail("PUBLICATION_INDEX_RECOVERY_TOPOLOGY")
        _digest(index["sha256"])
        if topology.get("topology_sha256") != _canonical_sha256(
            {key: item for key, item in topology.items() if key != "topology_sha256"}
        ):
            _fail("PUBLICATION_INDEX_RECOVERY_TOPOLOGY")
    before = original["candidate_checkout"]["index"]
    after = observed["candidate_checkout"]["index"]
    if (before["path"] != after["path"] or before["identity"] == after["identity"]
            or observed.get("candidate_index_matches_tree") is not True):
        _fail("PUBLICATION_INDEX_REPLACEMENT_REQUIRED")
    expected = _copy(original)
    expected["candidate_checkout"]["index"] = _copy(after)
    expected["topology_sha256"] = _canonical_sha256(
        {key: item for key, item in expected.items() if key != "topology_sha256"}
    )
    if expected != observed:
        _fail("PUBLICATION_INDEX_RECOVERY_TOPOLOGY")


def _validate_publication_profile_binding(
    profile: Any, request: Mapping[str, Any], execution_sha: str, *, code: str,
) -> None:
    """Validate the original inspector envelope; no file reads or execution grant."""
    _digest(execution_sha)
    expected = {
        "schema_version": "full_publication_profile_inspection.v1", "status": "PASS",
        "scope": "PROFILE_MANDATORY_AND_READINESS_IDENTITY",
        "candidate_sha": request["candidate_sha"],
        "transaction_sha256": request["publication_transaction_sha256"],
        "head_event_id": request["local_publication_event_id"], "execution_sha256": execution_sha,
        "dispatch_performed": False, "publication_performed": False,
    }
    if (not isinstance(profile, Mapping) or set(profile) != {*expected, "captures"}
            or any(profile.get(key) != value for key, value in expected.items())
            or profile["dispatch_performed"] is not False
            or profile["publication_performed"] is not False
            or not isinstance(profile.get("captures"), list) or not profile["captures"]):
        _fail(code)
    paths: set[str] = set()
    for row in profile["captures"]:
        if (not isinstance(row, Mapping) or set(row) != {"path", "size_bytes", "sha256"}
                or not isinstance(row["path"], str) or not Path(row["path"]).is_absolute()
                or not Path(row["path"]).is_relative_to(Path(request["cwd"]))
                or ".." in Path(row["path"]).parts or row["path"].casefold() in paths
                or type(row["size_bytes"]) is not int or row["size_bytes"] < 0):
            _fail(code)
        _digest(row["sha256"])
        paths.add(row["path"].casefold())


def _validate_unchanged_publication_observation(execution: Mapping[str, Any]) -> None:
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256

    attempts = execution.get("publication_attempts")
    if not isinstance(attempts, list) or not attempts:
        _fail("PUBLICATION_STABLE_OBSERVATION")
    attempt = attempts[-1]
    observation = execution.get("publication_stable_observation")
    if (isinstance(observation, Mapping)
            and observation.get("schema_version") == "workflow_publication_stable_observation.v4"):
        _validate_main_advanced_failed_observation(execution)
        return
    if (isinstance(observation, Mapping)
            and observation.get("schema_version") == "workflow_publication_stable_observation.v3"):
        _validate_published_observation(execution)
        return
    replaced = (
        isinstance(observation, Mapping)
        and observation.get("schema_version") == "workflow_publication_stable_observation.v2"
    )
    fields = {
        "schema_version", "stable_state", "request_sha256", "execution_sha256",
        "topology", "observer", "observed_at",
    } | ({"preparation_resolution"} if "main_preparation" in attempt else set()) | (
        {"git_launch_resolution"} if "git_launch" in attempt else set()
    ) | (
        {"original_topology", "profile_inspection"} if replaced else set()
    )
    if not isinstance(observation, Mapping) or set(observation) != fields or (
        observation["schema_version"] != (
            "workflow_publication_stable_observation.v2" if replaced
            else "workflow_publication_stable_observation.v1"
        ) or observation["stable_state"] != (
            "CANDIDATE_STABLE_INDEX_REPLACED" if replaced else "ORIGINAL_UNCHANGED"
        )
    ):
        _fail("PUBLICATION_STABLE_OBSERVATION")
    if (
        attempt["state"] != "RESULT_RECORDED" or attempt["result"]["status"] == "PASS"
        or observation["request_sha256"] != attempt["request_sha256"]
        or observation["execution_sha256"] != _canonical_sha256(attempt)
    ):
        _fail("PUBLICATION_STABLE_BINDING")
    _process(observation["observer"])
    if datetime.fromisoformat(observation["observed_at"]).tzinfo is None:
        _fail("PUBLICATION_STABLE_TIME")
    request = attempt["request"]
    topology = observation["topology"]
    if replaced:
        _require_index_replaced_topology(observation["original_topology"], topology)
        topology = observation["original_topology"]
        profile = observation["profile_inspection"]
        before_observation = {
            key: value for key, value in execution.items()
            if key != "publication_stable_observation"
        }
        _validate_publication_profile_binding(
            profile, request, _canonical_sha256(before_observation),
            code="PUBLICATION_INDEX_RECOVERY_PROFILE",
        )
    reconstructed_intent = {
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": request["candidate_sha"],
        "expected_main_sha": request["expected_main_sha"], "topology": topology,
        "dispatch_allowed": False, "publication_allowed": False,
    }
    if _canonical_sha256(reconstructed_intent) != request["local_publication_intent_sha256"]:
        _fail("PUBLICATION_STABLE_TOPOLOGY")
    if "main_preparation" in attempt:
        resolution = observation["preparation_resolution"]
        if (not isinstance(resolution, Mapping) or set(resolution) != {
            "main_preparation_sha256", "git_process", "lock_state",
        } or resolution["main_preparation_sha256"] != _canonical_sha256(
            attempt["main_preparation"]
        ) or resolution["lock_state"] != "ABSENT"):
            _fail("PUBLICATION_PREPARATION_UNRESOLVED")
        process = resolution["git_process"]
        if (not isinstance(process, Mapping) or set(process) not in (
            {"state"}, {"state", "winerror"},
        ) or process["state"] not in {"EXITED", "REUSED"}
                or ("winerror" in process and type(process["winerror"]) is not int)):
            _fail("PUBLICATION_PREPARATION_UNRESOLVED")


    if "git_launch" in attempt:
        resolution = observation["git_launch_resolution"]
        recovery = attempt.get("head_recovery")
        if recovery is not None:
            _validate_publication_head_recovery(attempt)
            if recovery["state"] != "RESTORED":
                _fail("PUBLICATION_GIT_LAUNCH_UNRESOLVED")
        if (not isinstance(resolution, Mapping) or set(resolution) != {
            "git_launch_sha256", "git_process", "auxiliary_state",
        } | ({"head_recovery_sha256"} if recovery is not None else set())
                or resolution["git_launch_sha256"] != _canonical_sha256(attempt["git_launch"])
                or resolution["auxiliary_state"] != (
                    "ORIGINAL_BYTES_RESTORED" if recovery is not None else "ORIGINAL_UNCHANGED"
                ) or recovery is not None
                and resolution["head_recovery_sha256"] != _canonical_sha256(recovery)):
            _fail("PUBLICATION_GIT_LAUNCH_UNRESOLVED")
        process = resolution["git_process"]
        if (not isinstance(process, Mapping) or set(process) not in (
            {"state"}, {"state", "winerror"},
        ) or process["state"] not in {"EXITED", "REUSED"}
                or ("winerror" in process and type(process["winerror"]) is not int)):
            _fail("PUBLICATION_GIT_LAUNCH_UNRESOLVED")


def _validate_main_advanced_failed_observation(execution: Mapping[str, Any]) -> None:
    """Bind failed N recovery to the unchanged original attempt and independent V(C)."""
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest

    attempt = execution["publication_attempts"][-1]
    record = execution["publication_stable_observation"]
    if (not isinstance(record, Mapping) or set(record) != {
        "schema_version", "stable_state", "request_sha256", "execution_sha256",
        "head_recovery_sha256", "recovery_observation", "profile_inspection", "git_process",
        "job_state", "observer", "observed_at", "dispatch_allowed", "publication_allowed",
    } or record["schema_version"] != "workflow_publication_stable_observation.v4"
            or record["stable_state"] != "CANDIDATE_RETAINED_MAIN_ADVANCED"
            or attempt["state"] != "RESULT_RECORDED" or attempt["result"]["status"] == "PASS"
            or "main_preparation" in attempt or "head_recovery" not in attempt
            or record["request_sha256"] != attempt["request_sha256"]
            or record["execution_sha256"] != canonical_digest(attempt)
            or record["head_recovery_sha256"] != canonical_digest(attempt["head_recovery"])
            or record["job_state"] not in {"EMPTY", "ABSENT"}
            or record["dispatch_allowed"] is not False
            or record["publication_allowed"] is not False):
        _fail("PUBLICATION_MAIN_ADVANCED_STABLE_OBSERVATION")
    recovery = attempt["head_recovery"]
    _validate_publication_head_recovery(attempt)
    if (recovery["schema_version"] != "workflow_publication_head_recovery.v2"
            or recovery["state"] != "RESTORED"
            or record["recovery_observation"] != recovery["scene_after"]):
        _fail("PUBLICATION_MAIN_ADVANCED_STABLE_RECOVERY")
    _process(record["observer"])
    process = record["git_process"]
    if (not isinstance(process, Mapping) or set(process) not in (
        {"state"}, {"state", "winerror"},
    ) or process["state"] not in {"EXITED", "REUSED"}
            or ("winerror" in process and type(process["winerror"]) is not int)):
        _fail("PUBLICATION_MAIN_ADVANCED_STABLE_PROCESS")
    instant = datetime.fromisoformat(record["observed_at"])
    if instant.tzinfo is None or instant < datetime.fromisoformat(recovery["completed_at"]):
        _fail("PUBLICATION_MAIN_ADVANCED_STABLE_TIME")
    before = {key: value for key, value in execution.items()
              if key != "publication_stable_observation"}
    _validate_publication_profile_binding(
        record["profile_inspection"], attempt["request"], canonical_digest(before),
        code="PUBLICATION_MAIN_ADVANCED_STABLE_PROFILE",
    )


def _validate_published_observation(execution: Mapping[str, Any]) -> None:
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256
    from ai_trading_system.platform.architecture.workflow_integration import (
        _publication_plan_metadata,
    )

    attempt = execution["publication_attempts"][-1]
    record = execution["publication_stable_observation"]
    if (not isinstance(record, Mapping) or set(record) != {
        "schema_version", "stable_state", "completion_basis", "request_sha256", "execution_sha256",
        "merge_observation", "profile_inspection", "git_process", "job_state", "observer",
        "observed_at",
    } or record["schema_version"] != "workflow_publication_stable_observation.v3"
            or record["stable_state"] != "LOCAL_PUBLISHED"
            or record["completion_basis"] not in {"ORIGINAL_GIT_EXIT_ZERO", "RECOVERED_STABLE_C"}
            or attempt["state"] != "RESULT_RECORDED" or attempt["result"]["status"] == "PASS"
            or record["request_sha256"] != attempt["request_sha256"]
            or record["execution_sha256"] != _canonical_sha256(attempt)
            or "git_merge" not in attempt or record["job_state"] not in {"EMPTY", "ABSENT"}):
        _fail("PUBLICATION_PUBLISHED_OBSERVATION")
    _process(record["observer"])
    process = record["git_process"]
    if (not isinstance(process, Mapping) or set(process) not in (
        {"state"}, {"state", "winerror"},
    ) or process["state"] not in {"EXITED", "REUSED"}
            or ("winerror" in process and type(process["winerror"]) is not int)):
        _fail("PUBLICATION_PUBLISHED_PROCESS")
    when = datetime.fromisoformat(record["observed_at"])
    if when.tzinfo is None or when < datetime.fromisoformat(attempt["exit"]["observed_at"]):
        _fail("PUBLICATION_PUBLISHED_TIME")
    before = {key: value for key, value in execution.items()
              if key != "publication_stable_observation"}
    _validate_publication_profile_binding(
        record["profile_inspection"], attempt["request"], _canonical_sha256(before),
        code="PUBLICATION_PUBLISHED_PROFILE",
    )
    merge = attempt["git_merge"]
    seen = {(row["reference_kind"] if row["kind"] == "reference-transaction" else row["kind"],
             row["stage"]) for row in merge["hooks"]}
    if not {("ORIG_HEAD", "prepared"), ("FAST_FORWARD", "prepared")}.issubset(seen):
        _fail("PUBLICATION_PUBLISHED_PREPARATION")
    if record["completion_basis"] == "ORIGINAL_GIT_EXIT_ZERO" and (
        merge["exit"] is None or merge["exit"]["returncode"] != 0
        or attempt["exit"]["returncode"] != 0 or not {
            ("ORIG_HEAD", "committed"), ("FAST_FORWARD", "committed"),
            ("post-merge", "0"), ("AUTO_MERGE", "committed"),
        }.issubset(seen)
    ):
        _fail("PUBLICATION_PUBLISHED_COMPLETION")
    observed = record["merge_observation"]
    if (not isinstance(observed, Mapping) or set(observed) != {
        "schema_version", "request_sha256", "plan_sha256", "checkouts", "worktree_inventory_sha256",
        "candidate_index_matches_tree", "main_sha", "auxiliary", "main_ref", "dispatch_allowed",
        "publication_allowed", "mutation_performed",
    } or observed["schema_version"] != "workflow_publication_merge_window_observation.v1"
            or observed["request_sha256"] != attempt["request_sha256"]
            or observed["plan_sha256"] != attempt["checkout_plan"]["plan"]["plan_sha256"]
            or observed["main_sha"] != attempt["request"]["candidate_sha"]
            or observed["candidate_index_matches_tree"] is not True
            or any(observed[key] is not False for key in (
                "dispatch_allowed", "publication_allowed", "mutation_performed",
            ))):
        _fail("PUBLICATION_PUBLISHED_SCENE")
    _digest(observed["worktree_inventory_sha256"])
    prepared = next(row["prepared_file"] for row in merge["hooks"]
                    if row["reference_kind"] == "FAST_FORWARD" and row["stage"] == "prepared")
    path = attempt["checkout_plan"]["plan"]["topology"]["main_ref"]["path"]
    if observed["main_ref"] != {**prepared, "path": path}:
        _fail("PUBLICATION_PUBLISHED_REF_IDENTITY")
    plan = attempt["checkout_plan"]["plan"]
    topology = plan["topology"]
    checkouts = observed["checkouts"]
    if not isinstance(checkouts, Mapping) or set(checkouts) != {
        row["role"] for row in plan["head_transitions"]
    }:
        _fail("PUBLICATION_PUBLISHED_SCENE")
    for transition in plan["head_transitions"]:
        role = transition["role"]
        expected = _copy(topology["candidate_checkout"] if role == "candidate_head"
                         else topology["main_checkout"])
        raw = bytes.fromhex(transition["after_hex"])
        expected["head"] = {**transition["before"], "bytes_hex": raw.hex(), "size": len(raw),
                            "sha256": hashlib.sha256(raw).hexdigest()}
        expected["observed_head"] = attempt["request"][
            "candidate_sha" if role == "candidate_head" else "expected_main_sha"
        ]
        current = checkouts[role]
        if not isinstance(current, Mapping):
            _fail("PUBLICATION_PUBLISHED_SCENE")
        if role == "candidate_head":
            index = current.get("index")
            if not isinstance(index, Mapping):
                _fail("PUBLICATION_PUBLISHED_INDEX")
            _publication_plan_metadata(index, Path(expected["index"]["path"]))
            if index["identity"] is None or index["size"] <= 0:
                _fail("PUBLICATION_PUBLISHED_INDEX")
            expected["index"] = index
        if current != expected:
            _fail("PUBLICATION_PUBLISHED_SCENE")
    auxiliary = observed["auxiliary"]
    orig_prepared = next(row["prepared_file"] for row in merge["hooks"]
                         if row["reference_kind"] == "ORIG_HEAD" and row["stage"] == "prepared")
    if (not isinstance(auxiliary, Mapping) or set(auxiliary) != {"orig_head", "reflogs"}
            or auxiliary["orig_head"] != {**orig_prepared, "path": plan["orig_head"]["path"]}
            or not isinstance(auxiliary["reflogs"], Mapping)
            or set(auxiliary["reflogs"]) != set(plan["reflogs"])):
        _fail("PUBLICATION_PUBLISHED_AUXILIARY")
    for role, before_log in plan["reflogs"].items():
        log = auxiliary["reflogs"][role]
        _publication_plan_metadata(log, Path(before_log["path"]))
        if (log["identity"] is None or log["size"] <= (before_log["size"] or 0)
                or (before_log["identity"] is not None
                    and log["identity"] != before_log["identity"])):
            _fail("PUBLICATION_PUBLISHED_AUXILIARY")


def _observe_unchanged_git_launch(attempt: Mapping[str, Any]) -> dict[str, Any] | None:
    """Observe the original Git's death and unchanged auxiliary state, read-only.

    The caller independently verifies the complete checkout topology (including
    both HEADs). This adds the original plan's ORIG_HEAD, reflogs and absence
    set; neither an exited worker nor an empty Job alone establishes no effects.
    """
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256
    from ai_trading_system.platform.architecture.workflow_execution import observe_process
    from ai_trading_system.platform.architecture.workflow_integration import (
        _local_publication_metadata,
        _require_publication_plan_absences,
    )

    if "git_launch" not in attempt:
        return None
    launch = attempt["git_launch"]
    process = observe_process(**launch["process"])
    if process["state"] not in {"EXITED", "REUSED"}:
        _fail("PUBLICATION_GIT_LAUNCH_NOT_TERMINAL")
    plan = attempt["checkout_plan"]["plan"]
    recovery = attempt.get("head_recovery")
    if recovery is not None:
        _validate_publication_head_recovery(attempt)
        if recovery["state"] != "RESTORED":
            _fail("PUBLICATION_HEAD_RECOVERY_INCOMPLETE")
    expected_orig = plan["orig_head"] if recovery is None else recovery["restored_orig_head"]
    _require_publication_plan_absences(plan["absent_paths"])
    if (_local_publication_metadata(Path(plan["orig_head"]["path"]), contents=True) != expected_orig
            or any(_local_publication_metadata(Path(record["path"])) != record
                   for record in plan["reflogs"].values())):
        _fail("PUBLICATION_GIT_LAUNCH_EFFECTS_REMAIN")
    _require_publication_plan_absences(plan["absent_paths"])
    return {"git_launch_sha256": _canonical_sha256(launch), "git_process": process,
            "auxiliary_state": ("ORIGINAL_UNCHANGED" if recovery is None
                                else "ORIGINAL_BYTES_RESTORED"),
            **({"head_recovery_sha256": _canonical_sha256(recovery)}
               if recovery is not None else {})}


def _observe_unchanged_preparation(attempt: Mapping[str, Any]) -> dict[str, Any] | None:
    """Read original Git identity and lock absence; never delete unknown residue."""
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256
    from ai_trading_system.platform.architecture.workflow_execution import observe_process

    if "main_preparation" not in attempt:
        return None
    preparation = attempt["main_preparation"]
    process = observe_process(**preparation["git_process"])
    if process["state"] not in {"EXITED", "REUSED"}:
        _fail("PUBLICATION_PREPARATION_GIT_NOT_TERMINAL")
    try:
        Path(preparation["prepared_ref"]["path"]).lstat()
    except FileNotFoundError:
        pass
    else:
        # Even an object matching the old bytes/identity needs a separate bounded
        # recovery action; an unrelated writer's replacement must be preserved.
        _fail("PUBLICATION_PREPARATION_LOCK_REMAINS")
    return {"main_preparation_sha256": _canonical_sha256(preparation),
            "git_process": process, "lock_state": "ABSENT"}


def _same_terminal_process_resolution(
    before: Mapping[str, Any] | None, after: Mapping[str, Any] | None,
) -> bool:
    """Compare stable evidence after independently observing the original process.

    EXITED (with/without ERROR_INVALID_PARAMETER) and REUSED all prove that the
    recorded PID + creation time is no longer live. Windows may change between
    these representations while the coordinator inspects the checkout. Keep
    the latest raw observation; do not treat PID recycling as authority drift.
    """
    if before is None or after is None:
        return before is after
    return (
        before.keys() == after.keys()
        and before["git_process"]["state"] in {"EXITED", "REUSED"}
        and after["git_process"]["state"] in {"EXITED", "REUSED"}
        and all(value == after[key] for key, value in before.items() if key != "git_process")
    )


def _validate_publication_head_recovery(execution: Mapping[str, Any]) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from ai_trading_system.platform.architecture.workflow_integration import (
        validate_publication_main_advanced_recovery_observation,
        validate_publication_recovery_observation,
    )

    record = execution["head_recovery"]
    if (not isinstance(record, Mapping) or set(record) != {
        "schema_version", "request_sha256", "checkout_effect_sha256", "git_merge_sha256",
        "state", "observer", "job_state", "git_process", "started_at", "completed_at",
        "scene_before", "scene_after", "restored_orig_head",
    } or record["schema_version"] not in {
        "workflow_publication_head_recovery.v1", "workflow_publication_head_recovery.v2",
    }
            or execution["state"] != "RESULT_RECORDED" or execution["result"]["status"] == "PASS"
            or "checkout_effect" not in execution or "git_launch" not in execution
            or record["request_sha256"] != execution["request_sha256"]
            or record["checkout_effect_sha256"] != canonical_digest(execution["checkout_effect"])
            or record["git_merge_sha256"] != (
                canonical_digest(execution["git_merge"]) if "git_merge" in execution else None
            ) or record["state"] not in {"INTENT", "RESTORED"}
            or record["job_state"] not in {"EMPTY", "ABSENT"}):
        _fail("PUBLICATION_HEAD_RECOVERY")
    _process(record["observer"])
    process = record["git_process"]
    if (not isinstance(process, Mapping) or set(process) not in (
        {"state"}, {"state", "winerror"},
    ) or process["state"] not in {"EXITED", "REUSED"}
            or ("winerror" in process and type(process["winerror"]) is not int)):
        _fail("PUBLICATION_HEAD_RECOVERY_PROCESS")
    started = datetime.fromisoformat(record["started_at"])
    if started.tzinfo is None or started < datetime.fromisoformat(execution["exit"]["observed_at"]):
        _fail("PUBLICATION_HEAD_RECOVERY_TIME")
    plan = execution["checkout_plan"]["plan"]
    advanced = record["schema_version"] == "workflow_publication_head_recovery.v2"
    validate_scene = (validate_publication_main_advanced_recovery_observation if advanced
                      else validate_publication_recovery_observation)
    expected_orig = validate_scene(
        record["scene_before"], execution["request"], plan, execution.get("git_merge"),
    )
    if record["restored_orig_head"] != expected_orig:
        _fail("PUBLICATION_HEAD_RECOVERY_ORIG_HEAD")
    if record["state"] == "INTENT":
        if record["completed_at"] is not None or record["scene_after"] is not None:
            _fail("PUBLICATION_HEAD_RECOVERY_COMPLETION")
        return
    completed = datetime.fromisoformat(record["completed_at"])
    if completed.tzinfo is None or completed < started:
        _fail("PUBLICATION_HEAD_RECOVERY_TIME")
    after = record["scene_after"]
    validate_scene(
        after, execution["request"], plan, execution.get("git_merge"), restored_orig=expected_orig,
    )
    if advanced:
        before = record["scene_before"]
        if (any(after[key] != before[key] for key in ("main_sha", "main_ref"))
                or after["auxiliary"]["reflogs"] != before["auxiliary"]["reflogs"]):
            _fail("PUBLICATION_HEAD_RECOVERY_MAIN_ADVANCED_CHANGED")
    if (after["auxiliary"]["orig_head"] != expected_orig or after["owned_locks"]
            or (not advanced and after["worktree_inventory_sha256"]
                != plan["topology"]["worktree_inventory_sha256"])
            or any(after["checkouts"][row["role"]]["head"] != row["before"]
                   for row in plan["head_transitions"]
                   if not advanced or row["role"] == "candidate_head")
            or after["checkouts"]["candidate_head"]["index"]
            != record["scene_before"]["checkouts"]["candidate_head"]["index"]):
        _fail("PUBLICATION_HEAD_RECOVERY_COMPLETION")


def _validate_publication_head_recovery_transition(
    old: Mapping[str, Any], value: Mapping[str, Any],
) -> None:
    previous, current = old.get("head_recovery"), value.get("head_recovery")
    if current == previous:
        return
    if (current is None or old["state"] != "RESULT_RECORDED"
            or {key: item for key, item in old.items() if key != "head_recovery"}
            != {key: item for key, item in value.items() if key != "head_recovery"}):
        _fail("PUBLICATION_HEAD_RECOVERY_TRANSITION")
    if previous is None:
        if current["state"] != "INTENT":
            _fail("PUBLICATION_HEAD_RECOVERY_TRANSITION")
    elif (previous["state"] != "INTENT" or current["state"] != "RESTORED"
          or {key: item for key, item in previous.items()
              if key not in {"state", "completed_at", "scene_after"}}
          != {key: item for key, item in current.items()
              if key not in {"state", "completed_at", "scene_after"}}):
        _fail("PUBLICATION_HEAD_RECOVERY_TRANSITION")


def _validate_publication_git_launch(execution: Mapping[str, Any]) -> None:
    """Validate one immutable suspended launch; this is never resume authority."""
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256

    record, request = execution["git_launch"], execution["request"]
    capsule = execution.get("hook_capsule")
    if (not isinstance(record, Mapping) or set(record) != {
        "schema_version", "request_sha256", "checkout_plan_sha256", "ready_sha256",
        "process", "worker_process", "launch_binding", "git_file_custodies",
        "pre_resume_sha256", "observed_at",
    } or record["schema_version"] != "workflow_publication_git_launch.v1"
            or request["schema_version"] != "workflow_execution_request.v5"
            or "main_preparation" in execution or not isinstance(capsule, Mapping)
            or not isinstance(capsule.get("ready"), Mapping)
            or execution["state"] not in {"RUNNING", "EXIT_CONFIRMED", "RESULT_RECORDED"}):
        _fail("PUBLICATION_GIT_LAUNCH_FIELDS")
    ready = capsule["ready"]
    plan = execution["checkout_plan"]["plan"]
    if (record["request_sha256"] != execution["request_sha256"]
            or record["checkout_plan_sha256"] != plan["plan_sha256"]
            or record["ready_sha256"] != _canonical_sha256(ready)
            or record["worker_process"] != capsule["worker_process"]):
        _fail("PUBLICATION_GIT_LAUNCH_BINDING")
    _process(record["process"])
    _process(record["worker_process"])
    if (record["process"] == record["worker_process"]
            or record["process"]["creation_time"] < record["worker_process"]["creation_time"]):
        _fail("PUBLICATION_GIT_LAUNCH_PROCESS")
    when = datetime.fromisoformat(record["observed_at"])
    if when.tzinfo is None or when < datetime.fromisoformat(ready["recorded_at"]):
        _fail("PUBLICATION_GIT_LAUNCH_TIME")
    launch = record["launch_binding"]
    if not isinstance(launch, Mapping) or set(launch) != {
        "argv", "cwd", "environment_sha256", "stdout_path",
    } or launch["cwd"] != request["cwd"]:
        _fail("PUBLICATION_GIT_LAUNCH_COMMAND")
    _digest(launch["environment_sha256"])
    argv = launch["argv"]
    if (not isinstance(argv, list) or len(argv) != 9 or not isinstance(argv[0], str)
            or not Path(argv[0]).is_absolute() or ".." in Path(argv[0]).parts
            or Path(argv[0]).name.casefold() != "git.exe"
            or Path(argv[0]).parent.name.casefold() not in {"cmd", "bin"}
            or argv[1:] != [
                "-c", "core.hooksPath=" + (
                    Path(request["cwd"]) / capsule["definition"]["directory"]
                ).as_posix(), "-c", "maintenance.auto=false", *plan["merge_argv_tail"],
            ] or launch["stdout_path"] != (
                Path(request["stdout_path"]).parent
                / ("publication-merge-" + execution["request_sha256"] + ".stdout")
            ).as_posix()):
        _fail("PUBLICATION_GIT_LAUNCH_COMMAND")
    root = _git_installation_root(Path(argv[0]))
    wanted = [Path(argv[0]), root / "mingw64/bin/git.exe", root / "usr/bin/sh.exe"]
    files = record["git_file_custodies"]
    if not isinstance(files, list) or len(files) != len(wanted):
        _fail("PUBLICATION_GIT_LAUNCH_FILES")
    namespace: dict[str, list[int]] = {}
    for row, path in zip(files, wanted, strict=True):
        if not isinstance(row, Mapping):
            _fail("PUBLICATION_GIT_LAUNCH_FILES")
        count = row.get("link_count", 1)
        fields = {"schema_version", "root", "relative", "root_identity", "parent_identities",
                  "identity", "size_bytes", "sha256"} | ({"link_count"} if count != 1 else set())
        relative = path.relative_to(root)
        parents = {Path(*relative.parts[:index]).as_posix()
                   for index in range(1, len(relative.parts))}
        if (set(row) != fields or type(count) is not int or count < 1
                or row["schema_version"] != (
                    "workflow_read_file_custody.v1" if count == 1
                    else "workflow_read_file_custody.v2"
                ) or row["root"] != root.as_posix() or row["relative"] != relative.as_posix()
                or type(row["size_bytes"]) is not int or not 0 < row["size_bytes"] <= 64 * 1024**2
                or not isinstance(row["parent_identities"], Mapping)
                or set(row["parent_identities"]) != parents):
            _fail("PUBLICATION_GIT_LAUNCH_FILES")
        _digest(row["sha256"])
        for relative_name, identity in {
            "": row["root_identity"], **row["parent_identities"], row["relative"]: row["identity"],
        }.items():
            if (not isinstance(identity, list) or len(identity) != 2
                    or any(type(part) is not int or part < 0 for part in identity)):
                _fail("PUBLICATION_GIT_LAUNCH_FILES")
            if relative_name in namespace and namespace[relative_name] != identity:
                _fail("PUBLICATION_GIT_LAUNCH_NAMESPACE")
            namespace[relative_name] = identity
    reconstructed = {
        "schema_version": "workflow_inherited_child_pre_resume.v2",
        "owner_resume_state": "NOT_RESUMED", "process": record["process"],
        "worker_process": record["worker_process"], "job_name": request["job_name"],
        "launch_binding": launch,
        "read_file_custodies": [*ready["inputs"]["read_file_custodies"], *files],
        "dispatch_allowed": False, "publication_allowed": False,
    }
    if record["pre_resume_sha256"] != _canonical_sha256(reconstructed):
        _fail("PUBLICATION_GIT_LAUNCH_PRE_RESUME")


def _validate_publication_checkout_effect(execution: Mapping[str, Any]) -> None:
    """A durable HEAD handoff is not a successful merge or a publication permit."""
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256

    record = execution["checkout_effect"]
    launch = execution.get("git_launch")
    if (not isinstance(launch, Mapping) or not isinstance(record, Mapping) or set(record) != {
        "schema_version", "request_sha256", "git_launch_sha256", "checkout_plan_sha256",
        "worker_process", "state", "peer_handoff_authorized", "started_at", "completed_at",
        "head_observations",
    } or record["schema_version"] != "workflow_publication_checkout_effect.v1"
            or execution["state"] not in {"RUNNING", "EXIT_CONFIRMED", "RESULT_RECORDED"}
            or record["state"] not in {"HEAD_HANDOFF_INTENT", "HEADS_SWITCHED"}):
        _fail("PUBLICATION_CHECKOUT_EFFECT_FIELDS")
    plan = execution["checkout_plan"]["plan"]
    if (record["request_sha256"] != execution["request_sha256"]
            or record["git_launch_sha256"] != _canonical_sha256(launch)
            or record["checkout_plan_sha256"] != plan["plan_sha256"]
            or record["worker_process"] != launch["worker_process"]
            or type(record["peer_handoff_authorized"]) is not bool
            or record["peer_handoff_authorized"] is not plan["peer_handoff_required"]):
        _fail("PUBLICATION_CHECKOUT_EFFECT_BINDING")
    instant = datetime.fromisoformat(record["started_at"])
    if instant.tzinfo is None or instant < datetime.fromisoformat(launch["observed_at"]):
        _fail("PUBLICATION_CHECKOUT_EFFECT_TIME")
    if record["state"] == "HEAD_HANDOFF_INTENT":
        if record["completed_at"] is not None or record["head_observations"] != []:
            _fail("PUBLICATION_CHECKOUT_EFFECT_INTENT")
        return
    if not isinstance(record["completed_at"], str):
        _fail("PUBLICATION_CHECKOUT_EFFECT_TIME")
    completed = datetime.fromisoformat(record["completed_at"])
    if completed.tzinfo is None or completed < instant:
        _fail("PUBLICATION_CHECKOUT_EFFECT_TIME")
    expected = []
    for transition in plan["head_transitions"]:
        identity = transition["before"]["identity"]
        if (not isinstance(identity, list) or len(identity) != 2
                or any(type(item) is not int or item < 0 for item in identity)):
            _fail("PUBLICATION_CHECKOUT_EFFECT_HEADS")
        raw = bytes.fromhex(transition["after_hex"])
        expected.append({
            "role": transition["role"], "path": transition["before"]["path"],
            "identity": transition["before"]["identity"], "bytes_hex": raw.hex(),
            "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
        })
    if record["head_observations"] != expected:
        _fail("PUBLICATION_CHECKOUT_EFFECT_HEADS")


def _validate_publication_checkout_effect_transition(
    old: Mapping[str, Any], value: Mapping[str, Any],
) -> None:
    previous, current = old.get("checkout_effect"), value.get("checkout_effect")
    if previous == current:
        return
    if (old["state"] != "RUNNING" or value["state"] != "RUNNING"
            or {key: item for key, item in old.items() if key != "checkout_effect"}
            != {key: item for key, item in value.items() if key != "checkout_effect"}
            or not isinstance(current, Mapping)):
        _fail("PUBLICATION_CHECKOUT_EFFECT_TRANSITION")
    if previous is None:
        if "git_launch" not in old or current["state"] != "HEAD_HANDOFF_INTENT":
            _fail("PUBLICATION_CHECKOUT_EFFECT_TRANSITION")
        return
    if (previous["state"] != "HEAD_HANDOFF_INTENT" or current["state"] != "HEADS_SWITCHED"
            or {key: item for key, item in previous.items()
                if key not in {"state", "completed_at", "head_observations"}}
            != {key: item for key, item in current.items()
                if key not in {"state", "completed_at", "head_observations"}}):
        _fail("PUBLICATION_CHECKOUT_EFFECT_TRANSITION")


def _validate_publication_git_merge(execution: Mapping[str, Any]) -> None:
    """Original resume intent and bounded native hook history, never worker PASS."""
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256
    from ai_trading_system.platform.architecture.workflow_integration import (
        _publication_plan_metadata,
        classify_publication_prepared_reference_update,
    )

    merge = execution["git_merge"]
    if (not isinstance(merge, Mapping) or set(merge) != {
        "schema_version", "request_sha256", "git_launch_sha256", "checkout_effect_sha256",
        "worker_process", "resume_intent_at", "hooks", "exit",
    } or merge["schema_version"] != "workflow_publication_git_merge.v1"
            or execution.get("checkout_effect", {}).get("state") != "HEADS_SWITCHED"
            or execution["state"] not in {"RUNNING", "EXIT_CONFIRMED", "RESULT_RECORDED"}):
        _fail("PUBLICATION_GIT_MERGE_FIELDS")
    launch, plan = execution["git_launch"], execution["checkout_plan"]["plan"]
    if (merge["request_sha256"] != execution["request_sha256"]
            or merge["git_launch_sha256"] != _canonical_sha256(launch)
            or merge["checkout_effect_sha256"] != _canonical_sha256(execution["checkout_effect"])
            or merge["worker_process"] != launch["worker_process"]):
        _fail("PUBLICATION_GIT_MERGE_BINDING")
    instant = datetime.fromisoformat(merge["resume_intent_at"])
    if (instant.tzinfo is None or instant < datetime.fromisoformat(
        execution["checkout_effect"]["completed_at"]
    )):
        _fail("PUBLICATION_GIT_MERGE_TIME")
    hooks = merge["hooks"]
    if not isinstance(hooks, list) or len(hooks) > 8:
        _fail("PUBLICATION_GIT_MERGE_HOOKS")
    seen: set[tuple[str, str]] = set()
    cleanup_identity = None
    for row in hooks:
        if not isinstance(row, Mapping) or set(row) - {"cleanup_lock"} != {
            "kind", "stage", "reference_kind", "updates_hex", "process_chain", "observed_at",
            "prepared_file",
        }:
            _fail("PUBLICATION_GIT_MERGE_HOOK_FIELDS")
        if "cleanup_lock" in row:
            if (row["reference_kind"] != "AUTO_MERGE"
                    or row["kind"] != "reference-transaction"
                    or ("post-merge", "0") not in seen):
                _fail("PUBLICATION_GIT_MERGE_LOCK")
            cleanup = row["cleanup_lock"]
            if cleanup is not None:
                cleanup_path = Path(plan["topology"]["candidate_checkout"]["common"]["path"])
                _publication_plan_metadata(
                    cleanup, cleanup_path / "packed-refs.lock", contents=True,
                )
                if (row["stage"] not in {"aborted", "prepared"}
                        or cleanup["identity"] is None or cleanup["bytes_hex"] != ""
                        or cleanup["size"] != 0
                        or (cleanup_identity is not None and cleanup != cleanup_identity)):
                    _fail("PUBLICATION_GIT_MERGE_LOCK")
                cleanup_identity = cleanup
            elif row["stage"] == "prepared":
                _fail("PUBLICATION_GIT_MERGE_LOCK")
        when = datetime.fromisoformat(row["observed_at"])
        if when.tzinfo is None or when < instant:
            _fail("PUBLICATION_GIT_MERGE_TIME")
        instant = when
        chain = row["process_chain"]
        if (not isinstance(chain, list) or not 2 <= len(chain) <= 8
                or chain[-1] != launch["process"]):
            _fail("PUBLICATION_GIT_MERGE_ORIGIN")
        for process in chain:
            _process(process)
        if (len({item["pid"] for item in chain}) != len(chain)
                or any(parent["creation_time"] > child["creation_time"]
                       for child, parent in zip(chain[:-1], chain[1:], strict=True))):
            _fail("PUBLICATION_GIT_MERGE_ORIGIN")
        if row["kind"] == "post-merge":
            if (row["stage"] != "0" or row["reference_kind"] is not None
                    or row["updates_hex"] != "" or row["prepared_file"] is not None
                    or ("FAST_FORWARD", "committed") not in seen):
                _fail("PUBLICATION_GIT_MERGE_POST")
            key = ("post-merge", "0")
        elif row["kind"] == "reference-transaction":
            raw = bytes.fromhex(row["updates_hex"])
            if raw.hex() != row["updates_hex"]:
                _fail("PUBLICATION_GIT_MERGE_UPDATES")
            reference = classify_publication_prepared_reference_update(
                plan, execution["request"], raw,
            )
            stage = row["stage"]
            if (reference != row["reference_kind"]
                    or stage not in {"prepared", "committed", "aborted"}):
                _fail("PUBLICATION_GIT_MERGE_UPDATES")
            key = (reference, stage)
            if ((reference == "FAST_FORWARD" and ("ORIG_HEAD", "committed") not in seen)
                    or (reference == "AUTO_MERGE" and ("FAST_FORWARD", "committed") not in seen)
                    or (stage == "committed" and (reference, "prepared") not in seen)):
                _fail("PUBLICATION_GIT_MERGE_ORDER")
            prepared = row["prepared_file"]
            if stage == "prepared" and reference in {"ORIG_HEAD", "FAST_FORWARD"}:
                checkout = plan["topology"]["candidate_checkout"]
                path = (Path(checkout["gitdir"]["path"]) / "ORIG_HEAD.lock"
                        if reference == "ORIG_HEAD"
                        else Path(checkout["common"]["path"]) / "refs/heads/main.lock")
                _publication_plan_metadata(prepared, path, contents=True)
                target = (execution["request"]["expected_main_sha"] if reference == "ORIG_HEAD"
                          else execution["request"]["candidate_sha"])
                if prepared["identity"] is None or bytes.fromhex(prepared["bytes_hex"]) != (
                    target + "\n"
                ).encode("ascii"):
                    _fail("PUBLICATION_GIT_MERGE_LOCK")
            elif prepared is not None:
                _fail("PUBLICATION_GIT_MERGE_LOCK")
        else:
            _fail("PUBLICATION_GIT_MERGE_HOOK_KIND")
        if key in seen:
            _fail("PUBLICATION_GIT_MERGE_DUPLICATE")
        seen.add(key)
    exit_fact = merge["exit"]
    if exit_fact is not None:
        if (not isinstance(exit_fact, Mapping) or set(exit_fact) != {
            "process", "worker_process", "returncode", "observed_at",
        } or exit_fact["process"] != launch["process"]
                or exit_fact["worker_process"] != launch["worker_process"]
                or type(exit_fact["returncode"]) is not int):
            _fail("PUBLICATION_GIT_MERGE_EXIT")
        when = datetime.fromisoformat(exit_fact["observed_at"])
        if when.tzinfo is None or when < instant:
            _fail("PUBLICATION_GIT_MERGE_TIME")


def _validate_publication_git_merge_transition(
    old: Mapping[str, Any], value: Mapping[str, Any],
) -> None:
    previous, current = old.get("git_merge"), value.get("git_merge")
    if previous == current:
        return
    if (old["state"] != "RUNNING" or value["state"] != "RUNNING"
            or {key: item for key, item in old.items() if key != "git_merge"}
            != {key: item for key, item in value.items() if key != "git_merge"}
            or not isinstance(current, Mapping)):
        _fail("PUBLICATION_GIT_MERGE_TRANSITION")
    if previous is None:
        if (old.get("checkout_effect", {}).get("state") != "HEADS_SWITCHED"
                or current["hooks"] != [] or current["exit"] is not None):
            _fail("PUBLICATION_GIT_MERGE_TRANSITION")
        return
    if (previous["exit"] is not None
            or {key: item for key, item in previous.items() if key not in {"hooks", "exit"}}
            != {key: item for key, item in current.items() if key not in {"hooks", "exit"}}):
        _fail("PUBLICATION_GIT_MERGE_TRANSITION")
    if current["exit"] is not None:
        if current["hooks"] != previous["hooks"]:
            _fail("PUBLICATION_GIT_MERGE_TRANSITION")
    elif (len(current["hooks"]) != len(previous["hooks"]) + 1
          or current["hooks"][:-1] != previous["hooks"]):
        _fail("PUBLICATION_GIT_MERGE_TRANSITION")


def _validate_main_preparation(execution: Mapping[str, Any]) -> None:
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256

    record = execution["main_preparation"]
    request = execution["request"]
    if not isinstance(record, Mapping) or set(record) != {
        "schema_version", "request_sha256", "topology", "worker_process", "git_process",
        "argv", "git_executable", "git_environment_sha256", "prepared_ref",
        "acknowledgements", "observed_at",
    } or record["schema_version"] != "workflow_publication_main_preparation.v1":
        _fail("PUBLICATION_PREPARATION_FIELDS")
    if (request["schema_version"] != "workflow_execution_request.v5"
            or execution["state"] not in {"RUNNING", "EXIT_CONFIRMED", "RESULT_RECORDED"}
            or record["request_sha256"] != execution["request_sha256"]):
        _fail("PUBLICATION_PREPARATION_BINDING")
    _process(record["worker_process"])
    _process(record["git_process"])
    executable = record["git_executable"]
    if (not isinstance(executable, Mapping)
            or set(executable) != {"path", "identity", "link_count", "sha256"}
            or not isinstance(executable["identity"], list) or len(executable["identity"]) != 2
            or any(type(part) is not int or part < 0 for part in executable["identity"])
            or type(executable["link_count"]) is not int or executable["link_count"] < 1):
        _fail("PUBLICATION_PREPARATION_EXECUTABLE")
    _digest(executable["sha256"])
    _digest(record["git_environment_sha256"])
    argv = record["argv"]
    if (not isinstance(argv, list) or len(argv) != 3 or not isinstance(argv[0], str)
            or not Path(argv[0]).is_absolute() or argv[1:] != ["update-ref", "--stdin"]
            or argv[0] != executable["path"]
            or record["acknowledgements"] != ["start: ok\n", "prepare: ok\n"]):
        _fail("PUBLICATION_PREPARATION_PROTOCOL")
    topology = record["topology"]
    intent = {
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": request["candidate_sha"],
        "expected_main_sha": request["expected_main_sha"], "topology": topology,
        "dispatch_allowed": False, "publication_allowed": False,
    }
    if _canonical_sha256(intent) != request["local_publication_intent_sha256"]:
        _fail("PUBLICATION_PREPARATION_TOPOLOGY")
    prepared = record["prepared_ref"]
    if (not isinstance(prepared, Mapping) or set(prepared) != {"path", "identity", "sha256"}
            or prepared["path"] != (
                Path(topology["candidate_checkout"]["common"]["path"]) / "refs/heads/main.lock"
            ).as_posix()
            or not isinstance(prepared["identity"], list) or len(prepared["identity"]) != 2
            or any(type(part) is not int or part < 0 for part in prepared["identity"])
            or prepared["sha256"] != hashlib.sha256(
                (request["candidate_sha"] + "\n").encode("ascii")
            ).hexdigest()):
        _fail("PUBLICATION_PREPARATION_REF")
    if datetime.fromisoformat(record["observed_at"]).tzinfo is None:
        _fail("PUBLICATION_PREPARATION_TIME")


def _validate_checkout_plan(execution: Mapping[str, Any]) -> None:
    from ai_trading_system.platform.architecture.workflow_integration import (
        validate_local_publication_checkout_plan,
    )

    record = execution["checkout_plan"]
    if (not isinstance(record, Mapping) or set(record) != {
        "schema_version", "request_sha256", "plan", "worker_process", "observed_at",
    } or record["schema_version"] != "workflow_publication_checkout_plan_binding.v1"):
        _fail("PUBLICATION_CHECKOUT_PLAN_FIELDS")
    if (execution["request"]["schema_version"] != "workflow_execution_request.v5"
            or execution["state"] not in {"RUNNING", "EXIT_CONFIRMED", "RESULT_RECORDED"}
            or record["request_sha256"] != execution["request_sha256"]):
        _fail("PUBLICATION_CHECKOUT_PLAN_BINDING")
    _process(record["worker_process"])
    if datetime.fromisoformat(record["observed_at"]).tzinfo is None:
        _fail("PUBLICATION_CHECKOUT_PLAN_TIME")
    validate_local_publication_checkout_plan(record["plan"], execution["request"])


def _validate_checkout_plan_transition(
    old: Mapping[str, Any], value: Mapping[str, Any],
) -> None:
    if "checkout_plan" in old:
        if value.get("checkout_plan") != old["checkout_plan"]:
            _fail("PUBLICATION_CHECKOUT_PLAN_CHANGED")
    elif "checkout_plan" in value and (
        old["state"] != "RUNNING" or value["state"] != "RUNNING"
        or "main_preparation" in old
        or {key: item for key, item in value.items() if key != "checkout_plan"} != old
    ):
        _fail("PUBLICATION_CHECKOUT_PLAN_TRANSITION")


def _publication_created_object_identity(descriptor: int, target: Path, kind: str) -> list[int]:
    """Observe the held creation fd, not a serialized identity or path reopen."""
    import ctypes
    import msvcrt
    import stat
    from ctypes import wintypes as w

    if type(descriptor) is not int or descriptor < 0 or kind not in {"directory", "file"}:
        _fail("PUBLICATION_HOOK_CREATION_DESCRIPTOR")
    info = os.fstat(descriptor)
    if (getattr(info, "st_file_attributes", 0) & 0x400
            or (kind == "directory" and not stat.S_ISDIR(info.st_mode))
            or (kind == "file" and (
                not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != 0
            ))):
        _fail("PUBLICATION_HOOK_CREATION_HANDLE")
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.GetFinalPathNameByHandleW.argtypes = [w.HANDLE, w.LPWSTR, w.DWORD, w.DWORD]
    api.GetFinalPathNameByHandleW.restype = w.DWORD
    name = ctypes.create_unicode_buffer(32768)
    length = api.GetFinalPathNameByHandleW(msvcrt.get_osfhandle(descriptor), name, len(name), 0)
    if not length or length >= len(name):
        _fail("PUBLICATION_HOOK_CREATION_HANDLE")
    path = name.value
    if path.startswith("\\\\?\\UNC\\"):
        path = "\\\\" + path[8:]
    elif path.startswith("\\\\?\\"):
        path = path[4:]
    if os.path.normcase(str(Path(path))) != os.path.normcase(str(target.absolute())):
        _fail("PUBLICATION_HOOK_CREATION_TARGET")
    return [info.st_dev, info.st_ino]


def _validate_hook_created_objects(execution: Mapping[str, Any]) -> None:
    record = execution["hook_capsule"]
    definition, objects = record["definition"], record["objects"]
    if not isinstance(objects, list) or len(objects) > 3:
        _fail("PUBLICATION_HOOK_CREATION_FIELDS")
    if not objects:
        return
    expected = [("directory", definition["directory"], None), *[
        ("file", row["path"], row["sha256"]) for row in definition["files"]
    ]]
    root_identity = execution["checkout_plan"]["plan"]["topology"]["candidate_checkout"][
        "root"
    ]["identity"]
    last_time = datetime.fromisoformat(record["observed_at"])
    for position, row in enumerate(objects):
        if not isinstance(row, Mapping) or set(row) != {
            "schema_version", "request_sha256", "definition_sha256", "kind", "path",
            "root_identity", "parent_identities", "file_identity", "worker_process",
            "observed_at", "target_sha256",
        }:
            _fail("PUBLICATION_HOOK_CREATION_FIELDS")
        if (row["schema_version"] != "workflow_publication_hook_created_object.v1"
                or row["request_sha256"] != record["request_sha256"]
                or row["definition_sha256"] != definition["definition_sha256"]
                or (row["kind"], row["path"], row["target_sha256"]) != expected[position]
                or row["root_identity"] != root_identity
                or row["worker_process"] != record["worker_process"]):
            _fail("PUBLICATION_HOOK_CREATION_BINDING")
        parts = Path(row["path"]).parts
        names = ["/".join(parts[:index]) for index in range(1, len(parts))]
        parents = row["parent_identities"]
        if not isinstance(parents, dict) or set(parents) != set(names):
            _fail("PUBLICATION_HOOK_CREATION_PARENTS")
        for pair in [row["root_identity"], row["file_identity"], *parents.values()]:
            if (not isinstance(pair, list) or len(pair) != 2
                    or any(type(item) is not int or item < 0 for item in pair)):
                _fail("PUBLICATION_HOOK_CREATION_IDENTITY")
        if position and parents != {
            **objects[0]["parent_identities"], definition["directory"]: objects[0]["file_identity"],
        }:
            _fail("PUBLICATION_HOOK_CREATION_NAMESPACE_CHANGED")
        instant = datetime.fromisoformat(row["observed_at"])
        if instant.tzinfo is None or instant < last_time:
            _fail("PUBLICATION_HOOK_CREATION_TIME")
        last_time = instant


def _validate_hook_ready(execution: Mapping[str, Any]) -> None:
    """Check the durable readset; original event transition supplies prior-hash authority."""
    from ai_trading_system.platform.architecture.workflow_contract import portable_path

    capsule, request = execution["hook_capsule"], execution["request"]
    ready = capsule["ready"]
    if (len(capsule["objects"]) != 3 or not isinstance(ready, Mapping) or set(ready) != {
        "schema_version", "request_sha256", "inputs", "recorded_at",
    } or ready["schema_version"] != "workflow_publication_hook_ready.v1"
            or ready["request_sha256"] != execution["request_sha256"]):
        _fail("PUBLICATION_HOOK_READY_FIELDS")
    inputs = ready["inputs"]
    if (not isinstance(inputs, Mapping) or set(inputs) != {
        "schema_version", "request_sha256", "definition_sha256", "worker_process", "observed_at",
        "profile_inspection", "runtime_identity", "runtime_file_count", "read_file_custodies",
        "dispatch_allowed", "publication_allowed", "resume_allowed",
    } or inputs["schema_version"] != "workflow_publication_live_inputs.v1"
            or inputs["request_sha256"] != execution["request_sha256"]
            or inputs["definition_sha256"] != capsule["definition"]["definition_sha256"]
            or inputs["worker_process"] != capsule["worker_process"]
            or any(inputs[key] is not False for key in (
                "dispatch_allowed", "publication_allowed", "resume_allowed",
            ))):
        _fail("PUBLICATION_HOOK_READY_INPUTS")
    recorded, observed = (datetime.fromisoformat(ready["recorded_at"]),
                          datetime.fromisoformat(inputs["observed_at"]))
    if (recorded.tzinfo is None or observed.tzinfo is None or recorded < observed
            or observed < datetime.fromisoformat(capsule["objects"][-1]["observed_at"])):
        _fail("PUBLICATION_HOOK_READY_TIME")
    profile = inputs["profile_inspection"]
    if not isinstance(profile, Mapping) or not isinstance(profile.get("execution_sha256"), str):
        _fail("PUBLICATION_HOOK_READY_PROFILE")
    _validate_publication_profile_binding(
        profile, request, profile["execution_sha256"], code="PUBLICATION_HOOK_READY_PROFILE",
    )
    runtime = inputs["runtime_identity"]
    if (not isinstance(runtime, Mapping) or set(runtime) != {
        "schema_version", "executable", "executable_sha256", "engine", "engine_sha256",
        "python_version", "implementation", "prefix", "base_prefix", "platform",
        "distribution_inventory_sha256", "distribution_count", "distribution_code",
        "environment_sha256",
    } or runtime["schema_version"] != "acceptance_runtime_identity.v1"
            or type(runtime["distribution_count"]) is not int or runtime["distribution_count"] < 0):
        _fail("PUBLICATION_HOOK_READY_RUNTIME")
    for key in ("executable", "engine", "prefix", "base_prefix"):
        if (not isinstance(runtime[key], str) or not Path(runtime[key]).is_absolute()
                or ".." in Path(runtime[key]).parts):
            _fail("PUBLICATION_HOOK_READY_RUNTIME")
    for key in ("executable_sha256", "engine_sha256", "distribution_inventory_sha256",
                "environment_sha256"):
        _digest(runtime[key])
    for key in ("python_version", "implementation", "platform"):
        if not isinstance(runtime[key], str) or not runtime[key]:
            _fail("PUBLICATION_HOOK_READY_RUNTIME")
    count = inputs["runtime_file_count"]
    files = inputs["read_file_custodies"]
    code = runtime["distribution_code"]
    if (type(count) is not int or not 2 <= count <= 20002
            or not isinstance(code, Mapping) or set(code) != {"sha256", "file_count", "size_bytes"}
            or type(code["file_count"]) is not int or code["file_count"] != count - 2
            or type(code["size_bytes"]) is not int or not 0 <= code["size_bytes"] <= 512 * 1024**2
            or not isinstance(files, list) or len(files) != count + len(profile["captures"]) + 4):
        _fail("PUBLICATION_HOOK_READY_INVENTORY")
    _digest(code["sha256"])
    paths: list[Path] = []
    namespace: dict[Path, tuple[str, list[int]]] = {}
    # Per-call lexical interning only: every supplied native identity is still
    # checked below. No filesystem observation or validation result is cached.
    roots: dict[str, Path] = {}
    directories: dict[tuple[str, str], Path] = {}
    for index, row in enumerate(files):
        if (not isinstance(row, Mapping) or set(row) != {
            "schema_version", "root", "relative", "root_identity", "parent_identities", "identity",
            "size_bytes", "sha256",
        } or row["schema_version"] != "workflow_read_file_custody.v1"
                or not isinstance(row["root"], str)
                or not isinstance(row["relative"], str)
                or portable_path(row["relative"]) != row["relative"]
                or type(row["size_bytes"]) is not int
                or not 0 <= row["size_bytes"] <= (
                    PUBLICATION_CAPTURE_BUDGET_BYTES
                    if index < count or count + 2 <= index < len(files) - 2
                    else 16 * 1024**2
                )):
            _fail("PUBLICATION_HOOK_READY_FILE")
        root_name = row["root"]
        if root_name not in roots:
            root = Path(root_name)
            if not root.is_absolute() or ".." in root.parts:
                _fail("PUBLICATION_HOOK_READY_FILE")
            roots[root_name] = root
        root = roots[root_name]
        parts = Path(row["relative"]).parts
        parents = row["parent_identities"]
        if (not isinstance(parents, dict)
                or set(parents) != {"/".join(parts[:n]) for n in range(1, len(parts))}):
            _fail("PUBLICATION_HOOK_READY_FILE")
        for pair in (row["root_identity"], row["identity"], *parents.values()):
            if (not isinstance(pair, list) or len(pair) != 2
                    or any(type(item) is not int or item < 0 for item in pair)):
                _fail("PUBLICATION_HOOK_READY_FILE")
        _digest(row["sha256"])
        file_path = root / row["relative"]
        observed_nodes = [
            (root, "directory", row["root_identity"]),
            (file_path, "file", row["identity"]),
        ]
        for name, pair in parents.items():
            directory_key = (root_name, name)
            if directory_key not in directories:
                directories[directory_key] = root / name
            observed_nodes.append((directories[directory_key], "directory", pair))
        for node_path, kind, pair in observed_nodes:
            if node_path in namespace and namespace[node_path] != (kind, pair):
                _fail("PUBLICATION_HOOK_READY_NAMESPACE")
            namespace[node_path] = (kind, pair)
        paths.append(file_path)
    for index, key in enumerate(("executable", "engine")):
        if paths[index] != Path(runtime[key]) or files[index]["sha256"] != runtime[key + "_sha256"]:
            _fail("PUBLICATION_HOOK_READY_RUNTIME")
    if (Path(capsule["definition"]["python_path"]) != Path(runtime["executable"])
            or Path(capsule["definition"]["entrypoint_path"]) not in {
                Path(row["path"]) for row in profile["captures"]
            }):
        _fail("PUBLICATION_HOOK_READY_ENTRYPOINT")
    distribution_paths = [str(path) for path in paths[2:count]]
    if (distribution_paths != sorted(distribution_paths)
            or len({path.casefold() for path in distribution_paths}) != len(distribution_paths)):
        _fail("PUBLICATION_HOOK_READY_RUNTIME")
    digest, total = hashlib.sha256(), 0
    for path, row in zip(distribution_paths, files[2:count], strict=True):
        total += row["size_bytes"]
        digest.update(json.dumps(
            [path, row["size_bytes"], row["sha256"]], separators=(",", ":"),
        ).encode())
        digest.update(b"\n")
    if digest.hexdigest() != code["sha256"] or total != code["size_bytes"]:
        _fail("PUBLICATION_HOOK_READY_RUNTIME")
    root_pair = execution["checkout_plan"]["plan"]["topology"]["candidate_checkout"]["root"][
        "identity"
    ]
    for row in files[count:-2]:
        if row["root"] != request["cwd"] or row["root_identity"] != root_pair:
            _fail("PUBLICATION_HOOK_READY_ROOT")
    for row, created, definition in zip(
        files[count:count + 2], capsule["objects"][1:], capsule["definition"]["files"], strict=True,
    ):
        if (row["relative"] != definition["path"] or row["sha256"] != definition["sha256"]
                or row["size_bytes"] != definition["size_bytes"]
                or row["identity"] != created["file_identity"]
                or row["parent_identities"] != created["parent_identities"]):
            _fail("PUBLICATION_HOOK_READY_CREATED_IDENTITY")
    for captured_path, row, capture in zip(
        paths[count + 2:-2], files[count + 2:-2], profile["captures"], strict=True,
    ):
        if (captured_path != Path(capture["path"]) or row["sha256"] != capture["sha256"]
                or row["size_bytes"] != capture["size_bytes"]):
            _fail("PUBLICATION_HOOK_READY_CAPTURE")
    if (paths[-2] != Path(request["publication_transaction_path"])
            or paths[-1] != Path(capsule["definition"]["policy_path"])):
        _fail("PUBLICATION_HOOK_READY_CONTROL_INPUT")
    for index in (-2, -1):
        expected_root = (Path(request["cwd"]) if paths[index].is_relative_to(Path(request["cwd"]))
                         else paths[index].parent)
        if (Path(files[index]["root"]) != expected_root
                or (expected_root == Path(request["cwd"])
                    and files[index]["root_identity"] != root_pair)):
            _fail("PUBLICATION_HOOK_READY_CONTROL_INPUT")


def _validate_hook_capsule(execution: Mapping[str, Any], *, actor: str) -> None:
    from ai_trading_system.platform.architecture.workflow_integration import (
        validate_local_publication_hook_capsule_definition,
    )

    record = execution["hook_capsule"]
    if (not isinstance(record, Mapping) or set(record) != {
        "schema_version", "request_sha256", "definition", "worker_process", "observed_at",
        "objects", "ready",
    } or record["schema_version"] != "workflow_publication_hook_capsule.v1"):
        _fail("PUBLICATION_HOOK_CAPSULE_FIELDS")
    if (execution["request"]["schema_version"] != "workflow_execution_request.v5"
            or execution["state"] not in {"RUNNING", "EXIT_CONFIRMED", "RESULT_RECORDED"}
            or record["request_sha256"] != execution["request_sha256"]
            or "checkout_plan" not in execution):
        _fail("PUBLICATION_HOOK_CAPSULE_BINDING")
    _process(record["worker_process"])
    plan = execution["checkout_plan"]
    if record["worker_process"] != plan["worker_process"]:
        _fail("PUBLICATION_HOOK_CAPSULE_WORKER")
    instant = datetime.fromisoformat(record["observed_at"])
    if instant.tzinfo is None or instant < datetime.fromisoformat(plan["observed_at"]):
        _fail("PUBLICATION_HOOK_CAPSULE_TIME")
    definition = record["definition"]
    validate_local_publication_hook_capsule_definition(
        definition, execution["request"], plan["plan"], actor=actor,
        policy_path=Path(definition["policy_path"]),
    )
    _validate_hook_created_objects(execution)
    # Creation alone is not readiness. The ready record is still not a live
    # execution capability; its first transition binds the real prior event.
    if record["ready"] is not None:
        _validate_hook_ready(execution)


def _validate_hook_capsule_transition(
    old: Mapping[str, Any], value: Mapping[str, Any],
) -> None:
    if "hook_capsule" in old:
        before, after = old["hook_capsule"], value.get("hook_capsule")
        if after == before:
            return
        if (isinstance(after, Mapping) and before["ready"] is None
                and after.get("ready") is not None):
            if (len(before["objects"]) != 3 or old["state"] != "RUNNING"
                    or value["state"] != "RUNNING" or "main_preparation" in old
                    or {key: item for key, item in after.items() if key != "ready"}
                    != {key: item for key, item in before.items() if key != "ready"}
                    or {key: item for key, item in value.items() if key != "hook_capsule"}
                    != {key: item for key, item in old.items() if key != "hook_capsule"}):
                _fail("PUBLICATION_HOOK_READY_TRANSITION")
            return
        if (not isinstance(after, Mapping)
                or {key: item for key, item in after.items() if key != "objects"}
                != {key: item for key, item in before.items() if key != "objects"}):
            _fail("PUBLICATION_HOOK_CAPSULE_CHANGED")
        if (old["state"] != "RUNNING" or value["state"] != "RUNNING"
                or "main_preparation" in old or before["ready"] is not None
                or not isinstance(after["objects"], list)
                or len(after["objects"]) != len(before["objects"]) + 1
                or after["objects"][:-1] != before["objects"]
                or {key: item for key, item in value.items() if key != "hook_capsule"}
                != {key: item for key, item in old.items() if key != "hook_capsule"}):
            _fail("PUBLICATION_HOOK_CREATION_TRANSITION")
    elif "hook_capsule" in value and (
        old["state"] != "RUNNING" or value["state"] != "RUNNING"
        or "checkout_plan" not in old or "main_preparation" in old
        or {key: item for key, item in value.items() if key != "hook_capsule"} != old
    ):
        _fail("PUBLICATION_HOOK_CAPSULE_TRANSITION")


def validate_execution(
    lease: ExecutionLease, *, _publication_parent: Mapping[str, Any] | None = None,
) -> None:
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256

    execution = lease.execution
    extended = (
        isinstance(execution, Mapping) and execution.get("schema_version") == "lease_execution.v2"
    )
    creations = (
        isinstance(execution, Mapping) and execution.get("schema_version") == "lease_execution.v3"
    )
    publication = (
        isinstance(execution, Mapping) and execution.get("schema_version") == "lease_execution.v5"
    )
    full_commitment = publication or (
        isinstance(execution, Mapping) and execution.get("schema_version") == "lease_execution.v4"
    )
    keys = (
        {
            "schema_version",
            "request",
            "request_sha256",
            "launcher",
            "state",
            "process",
            "exit",
            "result",
        }
        | ({"installation_attempts"} if extended else set())
        | ({"created_objects"} if creations else set())
        | ({"full_result_commitment"} if full_commitment else set())
        | ({"publication_attempts"} if publication else set())
        | ({"publication_stable_observation"}
           if publication and isinstance(execution, Mapping)
           and "publication_stable_observation" in execution else set())
        | ({"main_preparation"} if isinstance(execution, Mapping)
           and "main_preparation" in execution else set())
        | ({"checkout_plan"} if isinstance(execution, Mapping)
           and "checkout_plan" in execution else set())
        | ({"hook_capsule"} if isinstance(execution, Mapping)
           and "hook_capsule" in execution else set())
        | ({"git_launch"} if isinstance(execution, Mapping)
           and "git_launch" in execution else set())
        | ({"checkout_effect"} if isinstance(execution, Mapping)
           and "checkout_effect" in execution else set())
        | ({"git_merge"} if isinstance(execution, Mapping) and "git_merge" in execution else set())
        | ({"head_recovery"} if isinstance(execution, Mapping)
           and "head_recovery" in execution else set())
    )
    if not isinstance(execution, Mapping) or set(execution) != keys:
        _fail("FIELDS")
    if (
        execution["schema_version"]
        not in {
            "lease_execution.v1",
            "lease_execution.v2",
            "lease_execution.v3",
            "lease_execution.v4",
            "lease_execution.v5",
        }
        or execution["state"] not in _STATES
    ):
        _fail("SCHEMA_STATE")
    request = _request(execution["request"])
    if "checkout_plan" in execution:
        if _publication_parent is None or execution["schema_version"] != "lease_execution.v1":
            _fail("PUBLICATION_PARENT_REQUIRED")
        _validate_checkout_plan(execution)
    if "hook_capsule" in execution:
        if _publication_parent is None or execution["schema_version"] != "lease_execution.v1":
            _fail("PUBLICATION_PARENT_REQUIRED")
        _validate_hook_capsule(execution, actor=lease.actor)
    if "main_preparation" in execution:
        if _publication_parent is None or execution["schema_version"] != "lease_execution.v1":
            _fail("PUBLICATION_PARENT_REQUIRED")
        _validate_main_preparation(execution)
    if "git_launch" in execution:
        if _publication_parent is None or execution["schema_version"] != "lease_execution.v1":
            _fail("PUBLICATION_PARENT_REQUIRED")
        _validate_publication_git_launch(execution)
    if "checkout_effect" in execution:
        if _publication_parent is None or execution["schema_version"] != "lease_execution.v1":
            _fail("PUBLICATION_PARENT_REQUIRED")
        _validate_publication_checkout_effect(execution)
    if "git_merge" in execution:
        if _publication_parent is None or execution["schema_version"] != "lease_execution.v1":
            _fail("PUBLICATION_PARENT_REQUIRED")
        _validate_publication_git_merge(execution)
    if "head_recovery" in execution:
        if _publication_parent is None or execution["schema_version"] != "lease_execution.v1":
            _fail("PUBLICATION_PARENT_REQUIRED")
        _validate_publication_head_recovery(execution)
    if request["schema_version"] == "workflow_execution_request.v5":
        if _publication_parent is None or execution["schema_version"] != "lease_execution.v1":
            _fail("PUBLICATION_PARENT_REQUIRED")
        frozen = full_execution_projection(_publication_parent)
        if request["full_execution_sha256"] != _canonical_sha256(frozen) or any(
            request[key] != frozen["request"][key]
            for key in ("lease_id", "manifest_sha256", "candidate_sha", "cwd", "host_id",
                        "writer_epoch", "subject_task_id", "task_authority_sha256")
        ):
            _fail("PUBLICATION_FULL_BINDING")
        # Worker custody alone cannot attest stable checkout state. A later
        # independent adoption record is required before any PASS/release.
        if isinstance(execution["result"], Mapping) and execution["result"].get("status") == "PASS":
            _fail("PUBLICATION_STABLE_VERIFICATION_REQUIRED")
    if full_commitment:
        commitment = execution["full_result_commitment"]
        if (
            request["schema_version"] != "workflow_execution_request.v1"
            or execution["state"] not in {"EXIT_CONFIRMED", "RESULT_RECORDED"}
            or not isinstance(commitment, dict)
            or set(commitment) != {"record", "sha256"}
        ):
            _fail("FULL_COMMITMENT_BINDING")
        from ai_trading_system.platform.artifacts import canonical_json_bytes

        _digest(commitment["sha256"])
        if (
            hashlib.sha256(canonical_json_bytes(commitment["record"])).hexdigest()
            != commitment["sha256"]
        ):
            _fail("FULL_COMMITMENT_DIGEST")
        if not isinstance(execution["exit"], Mapping):
            _fail("FULL_COMMITMENT_BINDING")
        _full_record(commitment["record"], request, execution["exit"])
    if creations:
        records = execution["created_objects"]
        if request["schema_version"] != "workflow_execution_request.v4" or not isinstance(
            records, list
        ):
            _fail("CREATION_EXECUTION")
        if records and execution["state"] in {"RESERVED", "CONTAINED_SUSPENDED", "RESUME_INTENT"}:
            _fail("CREATION_STATE")
        names = set()
        from ai_trading_system.platform.architecture.workflow_contract import portable_path

        for record in records:
            directory_record = isinstance(record, dict) and record.get("kind") == "directory"
            if not isinstance(record, dict) or set(record) != {
                "plan_sha256",
                "root",
                "root_identity",
                "path",
                "file_identity",
                "worker_process",
            } | ({"kind"} if directory_record else {"target_sha256"}):
                _fail("CREATION_RECORD")
            if record["plan_sha256"] != request["installation_plan_sha256"]:
                _fail("CREATION_PLAN")
            if not directory_record:
                _digest(record["target_sha256"])
            _process(record["worker_process"])
            for key in ("root_identity", "file_identity"):
                identity = record[key]
                if (
                    not isinstance(identity, list)
                    or len(identity) != 2
                    or any(type(item) is not int or item < 0 for item in identity)
                    or identity[1] == 0
                ):
                    _fail("CREATION_IDENTITY")
            if record["root_identity"][0] != record["file_identity"][0]:
                _fail("CREATION_IDENTITY")
            root = record["root"]
            if (
                not isinstance(root, str)
                or not Path(root).is_absolute()
                or Path(root).as_posix() != root
            ):
                _fail("CREATION_ROOT")
            name = (root.casefold(), portable_path(record["path"]).casefold())
            if name in names:
                _fail("CREATION_DUPLICATE")
            names.add(name)
    if (
        request["lease_id"] != lease.lease_id
        or request["manifest_sha256"] != lease.change_manifest_sha256
        or request["subject_task_id"] != lease.task_id
        or execution["request_sha256"] != _canonical_sha256(request)
    ):
        _fail("REQUEST_BINDING")
    if request["schema_version"] == "workflow_execution_request.v2":
        if (
            request["source_head_sha"] != lease.base_commit
            or lease.change_id != "checkout:" + request["checkpoint_intent_id"]
            or sum(
                claim.kind == "contract"
                and claim.resource_id.startswith("checkout-source-only-capability:")
                and claim.access.value == "READ"
                for claim in lease.resources
            )
            != 1
        ):
            _fail("CHECKPOINT_LEASE_BINDING")
    if request["schema_version"] in {
        "workflow_execution_request.v3",
        "workflow_execution_request.v4",
    } and (
        request["source_head_sha"] != lease.base_commit
        or any(
            claim.resource_id.startswith("checkout-source-only-capability:")
            for claim in lease.resources
        )
    ):
        _fail("SOURCE_CANDIDATE_LEASE_BINDING")
    _process(execution["launcher"])
    state = execution["state"]
    if execution["process"] is not None:
        _process(execution["process"])
    elif state in {"CONTAINED_SUSPENDED", "RESUME_INTENT", "RUNNING"}:
        _fail("PROCESS_MISSING")
    if state in {"EXIT_CONFIRMED", "RESULT_RECORDED"}:
        exit_fact = execution["exit"]
        if not isinstance(exit_fact, dict) or set(exit_fact) != {
            "basis",
            "returncode",
            "observed_at",
            "job_state",
        }:
            _fail("EXIT_FACT")
        if exit_fact["basis"] not in {"LIVE_CONTAINED_HANDLE", "DEAD_LAUNCHER_JOB_EMPTY"}:
            _fail("EXIT_BASIS")
        if exit_fact["job_state"] not in {"EMPTY", "ABSENT"}:
            _fail("EXIT_JOB_STATE")
        if exit_fact["returncode"] is not None and type(exit_fact["returncode"]) is not int:
            _fail("EXIT_CODE")
        when = datetime.fromisoformat(exit_fact["observed_at"])
        if when.tzinfo is None:
            _fail("EXIT_TIME")
    elif execution["exit"] is not None:
        _fail("EARLY_EXIT_FACT")
    if state == "RESULT_RECORDED":
        result = execution["result"]
        if not isinstance(result, dict) or set(result) != {"status", "artifact", "reason"}:
            _fail("RESULT")
        if result["status"] not in {"PASS", "FAIL", "INSUFFICIENT", "INVALID"}:
            _fail("RESULT_STATUS")
        if not isinstance(result["reason"], str) or not result["reason"]:
            _fail("RESULT_REASON")
        if result["artifact"] is not None:
            artifact = result["artifact"]
            if not isinstance(artifact, dict) or set(artifact) != {"path", "sha256"}:
                _fail("RESULT_ARTIFACT")
            if artifact["path"] != request["result_path"]:
                _fail("RESULT_ARTIFACT_PATH")
            _digest(artifact["sha256"])
        if result["status"] == "PASS" and (
            execution["exit"]["returncode"] != 0 or result["artifact"] is None
        ):
            _fail("RESULT_PASS_WITHOUT_EXIT")
    elif execution["result"] is not None:
        _fail("EARLY_RESULT")
    if publication:
        attempts = execution["publication_attempts"]
        if (
            execution["state"] != "RESULT_RECORDED"
            or execution["result"]["status"] != "PASS"
            or not isinstance(attempts, list)
            or not attempts
        ):
            _fail("PUBLICATION_FULL_REQUIRED")
        for number, attempt in enumerate(attempts, 1):
            if not isinstance(attempt, Mapping):
                _fail("PUBLICATION_EXECUTION")
            validate_execution(replace(lease, execution=attempt), _publication_parent=execution)
            current = attempt["request"]
            if (
                current["schema_version"] != "workflow_execution_request.v5"
                or current["publication_attempt"] != number
            ):
                _fail("PUBLICATION_ATTEMPT")
            if number > 1:
                previous = attempts[number - 2]
                first = attempts[0]["request"]
                if (
                    previous["state"] != "RESULT_RECORDED"
                    or previous["result"]["status"] == "PASS"
                    or current["previous_publication_sha256"] != _canonical_sha256(previous)
                    or any(current[key] != first[key] for key in (
                        "publication_transaction_path", "publication_transaction_sha256",
                        "local_publication_event_id", "local_publication_intent_sha256",
                        "expected_main_sha", "full_execution_sha256",
                    ))
                ):
                    _fail("PUBLICATION_RECOVERY_BINDING")
        if "publication_stable_observation" in execution:
            _validate_unchanged_publication_observation(execution)
    if extended:
        attempts = execution["installation_attempts"]
        if (
            request["schema_version"] != "workflow_execution_request.v3"
            or state != "RESULT_RECORDED"
            or execution["result"]["status"] != "PASS"
            or not isinstance(attempts, list)
            or not attempts
        ):
            _fail("INSTALLATION_SOURCE_REQUIRED")
        for number, attempt in enumerate(attempts, 1):
            if not isinstance(attempt, Mapping) or attempt.get("schema_version") not in {
                "lease_execution.v1",
                "lease_execution.v3",
            }:
                _fail("INSTALLATION_EXECUTION")
            validate_execution(replace(lease, execution=attempt))
            current = attempt["request"]
            if (
                current["schema_version"] != "workflow_execution_request.v4"
                or current["installation_attempt"] != number
                or current["source_result_sha256"] != execution["result"]["artifact"]["sha256"]
                or any(
                    current[key] != request[key]
                    for key in (
                        "lease_id",
                        "manifest_sha256",
                        "cwd",
                        "host_id",
                        "writer_epoch",
                        "subject_task_id",
                        "task_authority_sha256",
                        "source_head_sha",
                        "source_request_sha256",
                        "source_transaction_sha256",
                        "review_sha256",
                    )
                )
            ):
                _fail("INSTALLATION_SOURCE_BINDING")
            if number > 1:
                previous = attempts[number - 2]
                first = attempts[0]["request"]
                # Recovery is a fresh execution, not reuse of a validation PASS.
                # Its immutable request must bind its actual launch environment;
                # ephemeral parent variables need not equal the first attempt.
                # Original source/runtime/plan/candidate custody remains required.
                if (
                    previous["state"] != "RESULT_RECORDED"
                    or previous["result"]["status"] == "PASS"
                    or current["previous_installation_sha256"] != _canonical_sha256(previous)
                    or any(
                        current[key] != first[key]
                        for key in (
                            "installed_candidate_sha",
                            "installation_plan_sha256",
                        )
                    )
                ):
                    _fail("INSTALLATION_RECOVERY_BINDING")


def execution_is_terminal(execution: Mapping[str, Any] | None) -> bool:
    """A source PASS does not release an unfinished checkout installation."""
    if execution is None:
        return True
    if execution.get("state") != "RESULT_RECORDED":
        return False
    if execution.get("schema_version") == "lease_execution.v5":
        # A failed attempt releases only after independently proving the exact
        # original checkout unchanged. Successful/partial publication needs its
        # distinct adopter; a worker PASS alone is still never sufficient.
        try:
            _validate_unchanged_publication_observation(execution)
        except (ParallelControlError, KeyError, TypeError, ValueError):
            return False
        return True
    if execution.get("schema_version") != "lease_execution.v2":
        return True
    attempts = execution.get("installation_attempts")
    return bool(
        attempts
        and attempts[-1]["state"] == "RESULT_RECORDED"
        and attempts[-1]["result"]["status"] == "PASS"
    )


def _validate_publication_ready_outer_transition(
    old: Mapping[str, Any], value: Mapping[str, Any],
) -> None:
    """Bind readiness only at its original append, not by rewriting later state."""
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256

    before, after = old.get("publication_attempts", []), value.get("publication_attempts", [])
    if not before or len(before) != len(after):
        return
    old_capsule, capsule = before[-1].get("hook_capsule"), after[-1].get("hook_capsule")
    if (not old_capsule or not capsule or old_capsule["ready"] is not None
            or capsule["ready"] is None):
        return
    expected = _copy(value)
    expected["publication_attempts"][-1]["hook_capsule"]["ready"] = None
    if (expected != old or capsule["ready"]["inputs"]["profile_inspection"]["execution_sha256"]
            != _canonical_sha256(old)):
        _fail("PUBLICATION_HOOK_READY_PRIOR_EXECUTION")


def validate_execution_transition(
    prior: ExecutionLease | None, current: ExecutionLease,
    *, _publication_parent: Mapping[str, Any] | None = None,
) -> None:
    validate_execution(current, _publication_parent=_publication_parent)
    _validate_checked_execution_transition(prior, current)


def _validate_checked_execution_transition(
    prior: ExecutionLease | None, current: ExecutionLease,
) -> None:
    """Check edges after the public entry validated the entire current tree.

    Recursive edges still enforce every transition rule. Their current child
    values were already validated by validate_execution's recursive traversal;
    validating those same values again neither observes disk nor adds authority.
    No checked state survives this call or crosses a history-event boundary.
    """
    if prior is None:
        _fail("GENESIS")
    immutable = (
        "lease_id",
        "task_id",
        "change_id",
        "lane_id",
        "actor",
        "base_commit",
        "change_manifest_sha256",
        "policy_version",
        "generation",
        "previous_lease_id",
        "requested_at",
        "acquired_at",
        "resources",
    )
    if any(getattr(prior, key) != getattr(current, key) for key in immutable):
        _fail("LEASE_IDENTITY_DRIFT")
    value = current.execution
    assert value is not None
    old = prior.execution
    if old is None:
        if prior.state != "ACTIVE" or value["state"] != "RESERVED":
            _fail("FIRST_STATE")
    else:
        old_publication = old.get("publication_attempts", [])
        publication = value.get("publication_attempts", [])
        if old_publication or publication:
            _validate_publication_ready_outer_transition(old, value)
            old_observation = old.get("publication_stable_observation")
            observation = value.get("publication_stable_observation")
            if old_observation is not None and value != old:
                _fail("PUBLICATION_STABLE_OBSERVATION_CHANGED")
            if observation is not None and old_observation is None and (
                publication != old_publication
            ):
                _fail("PUBLICATION_STABLE_TRANSITION")
            if (
                prior.state != "ACTIVE"
                or full_execution_projection(old) != full_execution_projection(value)
                or len(publication) not in {len(old_publication), len(old_publication) + 1}
            ):
                _fail("PUBLICATION_HISTORY_CHANGED")
            if current.state != "ACTIVE":
                if value != old or not execution_is_terminal(value):
                    _fail("PUBLICATION_HISTORY_CHANGED")
                return
            if len(publication) == len(old_publication) + 1:
                if publication[:-1] != old_publication or publication[-1]["state"] != "RESERVED":
                    _fail("PUBLICATION_TRANSITION")
                _validate_checked_execution_transition(
                    replace(prior, execution=None), replace(current, execution=publication[-1]),
                )
            else:
                if publication[:-1] != old_publication[:-1]:
                    _fail("PUBLICATION_HISTORY_CHANGED")
                _validate_checked_execution_transition(
                    replace(prior, execution=old_publication[-1]),
                    replace(current, execution=publication[-1]),
                )
            return
        if any(old[key] != value[key] for key in ("request", "request_sha256", "launcher")):
            _fail("REQUEST_DRIFT")
        _validate_checkout_plan_transition(old, value)
        _validate_hook_capsule_transition(old, value)
        _validate_publication_checkout_effect_transition(old, value)
        _validate_publication_git_merge_transition(old, value)
        _validate_publication_head_recovery_transition(old, value)
        if "git_launch" in old:
            if value.get("git_launch") != old["git_launch"]:
                _fail("PUBLICATION_GIT_LAUNCH_CHANGED")
        elif "git_launch" in value and (
            old["state"] != "RUNNING" or value["state"] != "RUNNING"
            or "main_preparation" in old
            or {key: item for key, item in value.items() if key != "git_launch"} != old
        ):
            _fail("PUBLICATION_GIT_LAUNCH_TRANSITION")
        if old.get("main_preparation") is not None:
            if value.get("main_preparation") != old["main_preparation"]:
                _fail("PUBLICATION_PREPARATION_CHANGED")
        elif "main_preparation" in value and (
            old["state"] != "RUNNING" or value["state"] != "RUNNING"
            or {key: item for key, item in value.items() if key != "main_preparation"} != old
        ):
            _fail("PUBLICATION_PREPARATION_TRANSITION")
        if old["process"] is not None and value["process"] != old["process"]:
            _fail("PROCESS_DRIFT")
        if old["exit"] is not None and value["exit"] != old["exit"]:
            _fail("EXIT_DRIFT")
        if old["result"] is not None and value["result"] != old["result"]:
            _fail("RESULT_DRIFT")
        if old.get("full_result_commitment") is not None and (
            value.get("full_result_commitment") != old["full_result_commitment"]
        ):
            _fail("FULL_COMMITMENT_DRIFT")
        if value["state"] != old["state"] and value["state"] not in _STATES[old["state"]]:
            _fail("STATE_TRANSITION")
        old_records = old.get("created_objects", [])
        records = value.get("created_objects", [])
        if old_records != records:
            if (
                prior.state != "ACTIVE"
                or current.state != "ACTIVE"
                or old["state"] != "RUNNING"
                or value["state"] != "RUNNING"
                or len(records) != len(old_records) + 1
                or records[:-1] != old_records
            ):
                _fail("CREATION_HISTORY_CHANGED")
        old_attempts = old.get("installation_attempts", [])
        attempts = value.get("installation_attempts", [])
        if old_attempts or attempts:
            if prior.state != "ACTIVE" or len(attempts) not in {
                len(old_attempts),
                len(old_attempts) + 1,
            }:
                _fail("INSTALLATION_TRANSITION")
            if len(attempts) == len(old_attempts) + 1:
                if attempts[:-1] != old_attempts or attempts[-1]["state"] != "RESERVED":
                    _fail("INSTALLATION_TRANSITION")
                _validate_checked_execution_transition(
                    replace(prior, execution=None), replace(current, execution=attempts[-1])
                )
            else:
                if attempts[:-1] != old_attempts[:-1]:
                    _fail("INSTALLATION_HISTORY_CHANGED")
                _validate_checked_execution_transition(
                    replace(prior, execution=old_attempts[-1]),
                    replace(current, execution=attempts[-1]),
                )
        elif value["schema_version"] != old["schema_version"]:
            if not (
                old["schema_version"] == "lease_execution.v1"
                and value["schema_version"] == "lease_execution.v4"
                and old["state"] == value["state"] == "EXIT_CONFIRMED"
                and prior.state == current.state == "ACTIVE"
            ):
                _fail("SCHEMA_STATE")
    if current.state != "ACTIVE" and not execution_is_terminal(value):
        _fail("PREMATURE_RELEASE")


def _git_installation_root(launcher: Path) -> Path:
    """Git for Windows root for a cmd/, bin/, mingw64/bin/ or usr/bin/ git.exe launcher.

    PATH order decides which launcher shutil.which returns; mingw64/bin/git.exe is two
    levels below the root, not one, so a fixed parent.parent would double mingw64.
    """
    parent = launcher.parent
    if parent.name.casefold() == "bin" and parent.parent.name.casefold() in {"mingw64", "usr"}:
        return parent.parent.parent
    return parent.parent


# Engineering bound for reads that overlap a live execution, not a model rule:
# the store arbiter guards only short critical sections (never a test run or a
# child wait), so a few seconds covers transient overlap; beyond it BUSY fails
# closed as before.
LIVE_EXECUTION_ARBITER_WAIT_SECONDS = 10.0
LIVE_EXECUTION_ARBITER_RETRY_SECONDS = 0.05


class ExecutionLifecycle:
    def __init__(self, store: FileExecutionLeaseStore) -> None:
        self.store = store

    def _head(self, lease_id: str, actor: str, *, require_active: bool = True) -> ExecutionLease:
        replay = self.store.replay()
        if replay.status != "PASS":
            _fail("REPLAY_INVALID")
        head = next((lease for lease in replay.lease_heads if lease.lease_id == lease_id), None)
        if head is None or (require_active and head.state != "ACTIVE") or head.actor != actor:
            _fail("ACTIVE_OWNER")
        return head

    def _append(
        self, head: ExecutionLease, value: Mapping[str, Any], now: datetime
    ) -> ExecutionLease:
        from ai_trading_system.platform.architecture.parallel_control_kernel import _lease_event

        updated = replace(head, execution=_copy(value))
        validate_execution_transition(head, updated)
        replay = self.store.replay()
        previous = dict(replay.head_event_ids)[head.lease_id]
        self.store._append_event(
            _lease_event(
                lease=updated,
                previous_event_id=previous,
                from_state="ACTIVE",
                to_state="ACTIVE",
                occurred_at=now,
                actor=head.actor,
                reason_codes=("EXECUTION_" + str(value["state"]),),
            )
        )
        return updated

    def require_checkpoint_worker(
        self, request: Mapping[str, Any], *, actor: str
    ) -> dict[str, Any]:
        """Fresh read-only child admission, not a serializable capability.

        Call at the entry/effect boundary. Reusing a returned witness cannot
        authorize another process, request, or a later execution state.
        """
        return self._require_worker(request, actor=actor, source_candidate=False)

    def require_source_candidate_worker(
        self, request: Mapping[str, Any], *, actor: str
    ) -> dict[str, Any]:
        """Recheck the bound v3 worker at each effect; the witness is not authority.

        The caller must separately verify its actual publication fence before
        effects. Generic execution custody does not grant publication authority.
        """
        return self._require_worker(request, actor=actor, source_candidate=True)

    def _require_worker(
        self,
        request: Mapping[str, Any],
        *,
        actor: str,
        source_candidate: bool,
        installation: bool = False,
        publication: bool = False,
    ) -> dict[str, Any]:
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_job_member,
            observe_process,
        )

        checked = _request(request)
        prefix = (
            "LOCAL_PUBLICATION"
            if publication
            else "SOURCE_INSTALLATION"
            if installation
            else "SOURCE_CANDIDATE"
            if source_candidate
            else "CHECKPOINT"
        )
        schema = (
            "workflow_execution_request.v5"
            if publication
            else "workflow_execution_request.v4"
            if installation
            else "workflow_execution_request.v3"
            if source_candidate
            else "workflow_execution_request.v2"
        )
        if checked["schema_version"] != schema:
            _fail(prefix + "_REQUEST_REQUIRED")
        with self._live_execution_arbiter_window(actor, operation="execution_worker_admit"):
            head = self._head(checked["lease_id"], actor)
            value = _copy(head.execution)
            if (
                value is None
                or value["request"] != checked
                or value["state"] not in {"RESUME_INTENT", "RUNNING"}
                or value["process"] is None
            ):
                _fail(prefix + "_WORKER_STATE")
        worker = current_job_member(checked["job_name"])
        if (
            worker == value["launcher"]
            or observe_process(**value["launcher"])["state"] != "RUNNING"
        ):
            _fail(prefix + "_LAUNCHER_UNPROVEN")
        with self._live_execution_arbiter_window(actor, operation="execution_worker_admit"):
            head = self._head(checked["lease_id"], actor)
            if head.execution != value:
                _fail(prefix + "_WORKER_STATE_CHANGED")
        return {
            "status": "PASS",
            "lease_id": checked["lease_id"],
            "execution_request_sha256": value["request_sha256"],
            "worker_process": worker,
            "effect_authority": (
                "SAME_BOUND_LEASE" if source_candidate or publication else "SAME_SOURCE_ONLY_LEASE"
            ),
        }

    @contextmanager
    def _live_execution_arbiter_window(
        self, actor: str, *, operation: str, now: datetime | None = None
    ) -> Iterator[None]:
        """Short read under the store arbiter, waiting briefly if it is busy.

        Used only where a caller runs concurrently with a live execution of the
        same request: the contained worker rechecking admission at each effect,
        and a duplicate request observing it (REPLAY_ONLY). Both hold the same
        arbiter for short critical sections; failing on transient overlap would
        kill a valid execution or misreport a replay. Exclusivity is unchanged:
        the lock is never stolen, and after the bounded wait BUSY fails closed.
        """
        import time

        deadline = time.monotonic() + LIVE_EXECUTION_ARBITER_WAIT_SECONDS
        with ExitStack() as stack:
            while True:
                try:
                    stack.enter_context(
                        self.store.atomic(
                            actor=actor, now=now or datetime.now(UTC), operation=operation
                        )
                    )
                    break
                except ParallelControlError as exc:
                    if exc.code != "LEASE_ARBITER_BUSY" or time.monotonic() >= deadline:
                        raise
                    time.sleep(LIVE_EXECUTION_ARBITER_RETRY_SECONDS)
            yield

    def reserve(
        self, request: Mapping[str, Any], *, actor: str, now: datetime | None = None
    ) -> dict[str, Any]:
        from ai_trading_system.platform.architecture.parallel_control_kernel import (
            _canonical_sha256,
            _lease_expiry,
        )
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
        )

        checked = _request(request)
        if checked["schema_version"] == "workflow_execution_request.v4":
            _fail("INSTALLATION_PARENT_REQUIRED")
        if checked["schema_version"] == "workflow_execution_request.v5":
            _fail("PUBLICATION_PARENT_REQUIRED")
        instant = now or datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_reserve"):
            head = self._head(checked["lease_id"], actor, require_active=False)
            for other in self.store.replay().lease_heads:
                if (
                    other.lease_id != head.lease_id
                    and other.execution is not None
                    and other.execution["request"]["request_id"] == checked["request_id"]
                ):
                    _fail("REQUEST_REUSE_MISMATCH")
            if head.execution is not None:
                if head.execution["request"] != checked:
                    _fail("REQUEST_REUSE_MISMATCH")
                return {
                    "status": "REPLAY_ONLY",
                    "dispatch_allowed": False,
                    "execution": _copy(head.execution),
                }
            if head.state != "ACTIVE" or _lease_expiry(head) <= instant:
                _fail("LEASE_EXPIRED")
            value = {
                "schema_version": "lease_execution.v1",
                "request": checked,
                "request_sha256": _canonical_sha256(checked),
                "launcher": current_process_identity(),
                "state": "RESERVED",
                "process": None,
                "exit": None,
                "result": None,
            }
            updated = self._append(head, value, instant)
            return {
                "status": "RESERVED",
                "dispatch_allowed": True,
                "execution": _copy(updated.execution),
            }

    def _live_binding(self, head: ExecutionLease, handle: WindowsJobProcess) -> dict[str, Any]:
        from ai_trading_system.platform.architecture.workflow_execution import WindowsJobProcess

        if not isinstance(handle, WindowsJobProcess) or head.execution is None:
            _fail("LIVE_HANDLE_REQUIRED")
        execution = _copy(head.execution)
        identity = handle.identity()
        if execution["launcher"] != {
            "pid": identity["launcher_pid"],
            "creation_time": identity["launcher_creation_time"],
        }:
            _fail("LAUNCHER_MISMATCH")
        if identity["job_name"] != execution["request"]["job_name"]:
            _fail("JOB_MISMATCH")
        for key, value in handle.launch_binding().items():
            if execution["request"].get(key) != value:
                _fail("LAUNCH_BINDING_MISMATCH")
        process = {"pid": identity["pid"], "creation_time": identity["creation_time"]}
        if execution["process"] is not None and execution["process"] != process:
            _fail("PROCESS_MISMATCH")
        execution["process"] = process
        return dict(execution)

    def bind(self, lease_id: str, handle: WindowsJobProcess, *, actor: str) -> None:
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_bind"):
            head = self._head(lease_id, actor)
            value = self._live_binding(head, handle)
            if value["state"] != "RESERVED" or handle.poll() is not None or handle._resumed:
                _fail("BIND_STATE")
            value["state"] = "CONTAINED_SUSPENDED"
            self._append(head, value, instant)

    def resume(self, lease_id: str, handle: WindowsJobProcess, *, actor: str) -> dict[str, Any]:
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_resume"):
            head = self._head(lease_id, actor)
            value = self._live_binding(head, handle)
            if value["state"] in {"RESUME_INTENT", "RUNNING", "EXIT_CONFIRMED", "RESULT_RECORDED"}:
                return {"status": "REPLAY_ONLY", "dispatch_allowed": False}
            if value["state"] != "CONTAINED_SUSPENDED":
                _fail("RESUME_STATE")
            value["state"] = "RESUME_INTENT"
            head = self._append(head, value, instant)
            handle.resume()  # Local syscall only; no child wait under the short arbiter.
            value["state"] = "RUNNING"
            self._append(head, value, datetime.now(UTC))
            return {"status": "RUNNING", "dispatch_allowed": False}

    def confirm_exit(self, lease_id: str, handle: WindowsJobProcess, *, actor: str) -> None:
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            head = self._head(lease_id, actor)
            value = self._live_binding(head, handle)
            if value["state"] in {"EXIT_CONFIRMED", "RESULT_RECORDED"}:
                return
            code = handle.poll()
            if code is None or handle.active_process_count() != 0:
                _fail("STILL_RUNNING")
            value["state"] = "EXIT_CONFIRMED"
            value["exit"] = {
                "basis": "LIVE_CONTAINED_HANDLE",
                "returncode": code,
                "observed_at": instant.isoformat(),
                "job_state": "EMPTY",
            }
            self._append(head, value, instant)

    def commit_full_result(
        self,
        lease_id: str,
        *,
        actor: str,
        record: Mapping[str, Any],
    ) -> None:
        """Freeze the original launcher's validated Full result before artifact I/O."""
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
        )
        from ai_trading_system.platform.artifacts import canonical_json_bytes

        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            head = self._head(lease_id, actor)
            value = _copy(head.execution)
            if (
                value is None
                or value["state"] not in {"EXIT_CONFIRMED", "RESULT_RECORDED"}
                or value["request"]["schema_version"] != "workflow_execution_request.v1"
                or value["launcher"] != current_process_identity()
                or value["exit"]["basis"] != "LIVE_CONTAINED_HANDLE"
            ):
                _fail("FULL_COMMITMENT_OWNER")
            _full_record(dict(record), value["request"], value["exit"])
            commitment = {
                "record": dict(record),
                "sha256": hashlib.sha256(canonical_json_bytes(dict(record))).hexdigest(),
            }
            if value.get("full_result_commitment") is not None:
                if value["full_result_commitment"] != commitment:
                    _fail("FULL_COMMITMENT_DRIFT")
                return
            if value["state"] != "EXIT_CONFIRMED":
                _fail("FULL_COMMITMENT_STATE")
            value["schema_version"] = "lease_execution.v4"
            value["full_result_commitment"] = commitment
            self._append(head, value, instant)

    def record_result(
        self,
        lease_id: str,
        *,
        actor: str,
        result_path: Path,
        expected_sha256: str | None = None,
    ) -> None:
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
        from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            head = self._head(lease_id, actor)
            if head.execution is None:
                _fail("MISSING")
            value = _copy(head.execution)
            if value["state"] not in {"EXIT_CONFIRMED", "RESULT_RECORDED"}:
                _fail("RESULT_STATE")
            if result_path.absolute().as_posix() != value["request"]["result_path"]:
                _fail("RESULT_PATH")
            # Formal adoption remains a separate independent validator. This is
            # result custody AFTER actual exit, never permission to publish.
            content = bounded_regular_bytes(result_path)
            if expected_sha256 is not None:
                _digest(expected_sha256)
                if hashlib.sha256(content).hexdigest() != expected_sha256:
                    _fail("RESULT_ADOPTION_CHANGED")
            result = load_strict_json_text(content.decode("utf-8"))
            commitment = value.get("full_result_commitment")
            if commitment is not None:
                if hashlib.sha256(content).hexdigest() != commitment["sha256"]:
                    _fail("FULL_COMMITMENT_RESULT_CHANGED")
                summary = commitment["record"].get("summary")
                if not isinstance(summary, dict) or set(summary) != {"path", "sha256"}:
                    _fail("FULL_COMMITMENT_SUMMARY")
                expected_summary = result_path.parent / "test_runtime_summary.json"
                if summary["path"] != expected_summary.absolute().as_posix() or (
                    hashlib.sha256(bounded_regular_bytes(expected_summary)).hexdigest()
                    != summary["sha256"]
                ):
                    _fail("FULL_COMMITMENT_SUMMARY_CHANGED")
            request = value["request"]
            if not isinstance(result, dict) or any(
                result.get(key) != item for key, item in _result_binding(request).items()
            ):
                _fail("RESULT_BINDING")
            if request["schema_version"] == "workflow_execution_request.v2" and (
                "candidate_sha" in result or "validation_identity_sha256" in result
            ):
                _fail("CHECKPOINT_RESULT_KIND")
            if request["schema_version"] in {
                "workflow_execution_request.v3",
                "workflow_execution_request.v4",
            } and ("candidate_sha" in result or "validation_identity_sha256" in result):
                _fail("SOURCE_CANDIDATE_RESULT_KIND")
            if (
                request["schema_version"] in {
                    "workflow_execution_request.v4", "workflow_execution_request.v5",
                }
                and result.get("status") == "PASS"
            ):
                # Generic worker-result custody cannot attest checkout stability.
                # DEVX-015 V3 requires independent ref/index/worktree adoption;
                # a self-reported stable_state must never release this lease.
                _fail("PUBLICATION_STABLE_VERIFICATION_REQUIRED"
                      if request["schema_version"] == "workflow_execution_request.v5"
                      else "INSTALLATION_STABLE_VERIFICATION_REQUIRED")
            value["state"] = "RESULT_RECORDED"
            value["result"] = {
                "status": result.get("status"),
                "reason": "RUNNER_RESULT",
                "artifact": {
                    "path": result_path.absolute().as_posix(),
                    "sha256": hashlib.sha256(content).hexdigest(),
                },
            }
            if head.execution["state"] == "RESULT_RECORDED":
                if value != head.execution:
                    _fail("RESULT_REPLAY_MISMATCH")
                return
            self._append(head, value, instant)

    def record_incomplete_result(self, lease_id: str, *, actor: str) -> None:
        """Close custody after proven live-handle exit, never fabricate a result.

        The caller preserves any missing/invalid artifact and independently
        decides its failed task disposition. No dispatch or PASS follows here.
        """
        instant = datetime.now(UTC)
        incomplete = {
            "status": "INSUFFICIENT",
            "artifact": None,
            "reason": "RUNNER_RESULT_UNAVAILABLE_OR_INVALID",
        }
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            head = self._head(lease_id, actor)
            value = _copy(head.execution)
            if value is None or value["state"] not in {"EXIT_CONFIRMED", "RESULT_RECORDED"}:
                _fail("RESULT_STATE")
            if value["exit"]["basis"] != "LIVE_CONTAINED_HANDLE":
                _fail("LIVE_EXIT_REQUIRED")
            if value["state"] == "RESULT_RECORDED":
                if value["result"] != incomplete:
                    _fail("RESULT_REPLAY_MISMATCH")
                return
            value["state"] = "RESULT_RECORDED"
            value["result"] = incomplete
            self._append(head, value, instant)

    def recover(self, lease_id: str, *, actor: str, action: str = "observe") -> dict[str, Any]:
        from ai_trading_system.platform.architecture.workflow_execution import (
            observe_job,
            observe_process,
        )

        if action not in {"observe", "terminate_frozen_job"}:
            _fail("RECOVERY_ACTION")
        instant = datetime.now(UTC)
        with self._live_execution_arbiter_window(
            actor, operation="execution_complete", now=instant
        ):
            head = self._head(lease_id, actor, require_active=False)
            if head.execution is None:
                _fail("MISSING")
            value = _copy(head.execution)
            if value["state"] == "RESULT_RECORDED":
                return {"status": "REPLAY_ONLY", "execution": value}
            if head.state != "ACTIVE":
                _fail("ACTIVE_OWNER")
        # OS observation/termination waits are outside the short arbiter. The
        # immutable lifecycle request still prevents expiry/reassignment/launch.
        launcher = observe_process(**value["launcher"])
        if launcher["state"] not in {"EXITED", "REUSED"}:
            return {
                "status": "OBSERVE_ONLY",
                "launcher": launcher,
                "allowed_actions": ["observe", "launcher_owned_complete"],
            }
        job = observe_job(value["request"]["job_name"])
        if job["state"] == "ACTIVE" and action == "terminate_frozen_job":
            if value["process"] is None:
                _fail("RECOVERY_PROCESS_UNPROVEN")
            job = observe_job(
                value["request"]["job_name"],
                terminate=True,
                expected_process=value["process"],
            )
        if job["state"] not in {"EMPTY", "ABSENT"}:
            return {
                "status": "RECOVERY_REQUIRED",
                "job": job,
                "allowed_actions": ["observe", "terminate_frozen_job"],
            }
        with self.store.atomic(actor=actor, now=datetime.now(UTC), operation="execution_complete"):
            head = self._head(lease_id, actor, require_active=False)
            if head.execution is not None and head.execution["state"] == "RESULT_RECORDED":
                return {"status": "REPLAY_ONLY", "execution": _copy(head.execution)}
            if head.state != "ACTIVE" or head.execution != value:
                _fail("RECOVERY_STATE_CHANGED")
            if value["state"] != "EXIT_CONFIRMED":
                value["state"] = "EXIT_CONFIRMED"
                value["exit"] = {
                    "basis": "DEAD_LAUNCHER_JOB_EMPTY",
                    "returncode": None,
                    "observed_at": instant.isoformat(),
                    "job_state": job["state"],
                }
                head = self._append(head, value, instant)
            source_adoption_missing = value["request"]["schema_version"] in {
                "workflow_execution_request.v3",
                "workflow_execution_request.v4",
                "workflow_execution_request.v5",
            }
            full_adoption_missing = (
                value["request"]["schema_version"] == "workflow_execution_request.v1"
                and value.get("full_result_commitment") is None
            )
            if (
                value["exit"]["returncode"] is not None
                and not source_adoption_missing
                and not full_adoption_missing
            ):
                planned_result = Path(value["request"]["result_path"])
                try:
                    self.record_result(lease_id, actor=actor, result_path=planned_result)
                except (OSError, ValueError, ParallelControlError):
                    # Preserve invalid/missing bytes for independent inspection.
                    # Such a result is never promoted to PASS or redispatched.
                    if value.get("full_result_commitment") is not None:
                        return {
                            "status": "RECOVERY_REQUIRED",
                            "reason": "FULL_COMMITTED_RESULT_REQUIRES_VERIFIED_RECOVERY",
                            "allowed_actions": ["observe", "full_recovery"],
                            "dispatch_allowed": False,
                        }
                    pass
                else:
                    return {
                        "status": "RECOVERED_TERMINAL",
                        "execution": _copy(self._head(lease_id, actor).execution),
                        "dispatch_allowed": False,
                    }
            # No observed exit code/result closure means no PASS and no redispatch.
            value["state"] = "RESULT_RECORDED"
            value["result"] = {
                "status": "INSUFFICIENT",
                "artifact": None,
                "reason": (
                    "SOURCE_ADOPTION_INCOMPLETE"
                    if source_adoption_missing
                    else "RECOVERED_WITHOUT_COMPLETE_RUNNER_RESULT"
                ),
            }
            self._append(head, value, datetime.now(UTC))
            return {"status": "RECOVERED_TERMINAL", "execution": value, "dispatch_allowed": False}


class PublicationLifecycle(ExecutionLifecycle):
    """Contained publication attempts within the original Full/publication lease.

    This does not create a publisher, permit arbitrary Git effects or attest a
    stable checkout. Original Full custody remains immutable; failed attempts
    remain held until the independent publication recovery/adopter completes.
    """

    def __init__(self, fence: IntegrationPublicationFence) -> None:
        from ai_trading_system.platform.architecture.integration_publication_fence import (
            IntegrationPublicationFence,
        )

        if not isinstance(fence, IntegrationPublicationFence):
            _fail("PUBLICATION_FENCE_REQUIRED")
        # Reuse this exact instance, including its nested atomic context. Opening
        # another store against the same path can deadlock the original arbiter.
        super().__init__(fence.guard.store)
        self.fence = fence

    def _head(self, lease_id: str, actor: str, *, require_active: bool = True) -> ExecutionLease:
        physical = super()._head(lease_id, actor, require_active=require_active)
        if physical.execution is None or not physical.execution.get("publication_attempts"):
            _fail("PUBLICATION_MISSING")
        return replace(physical, execution=_copy(physical.execution["publication_attempts"][-1]))

    def _append(
        self, head: ExecutionLease, value: Mapping[str, Any], now: datetime,
    ) -> ExecutionLease:
        physical = super()._head(head.lease_id, head.actor)
        outer = _copy(physical.execution)
        if not outer or outer.get("publication_attempts", [None])[-1] != head.execution:
            _fail("PUBLICATION_STATE_CHANGED")
        outer["publication_attempts"][-1] = _copy(value)
        updated = super()._append(physical, outer, now)
        return replace(updated, execution=_copy(value))

    def _require_original_publication(
        self, checked: Mapping[str, Any], actor: str, *, head_handoff: bool = False,
        merge_window: bool = False, recovery_window: bool = False,
        auto_merge_cleanup: Mapping[str, Any] | None = None,
        main_advanced_recovery: bool = False,
    ) -> dict[str, Any]:
        from ai_trading_system.platform.architecture.parallel_control_kernel import (
            _canonical_sha256,
        )

        if checked["schema_version"] != "workflow_execution_request.v5":
            _fail("PUBLICATION_REQUEST_REQUIRED")
        transaction = Path(checked["publication_transaction_path"])
        if sum((head_handoff, merge_window, recovery_window)) > 1:
            _fail("PUBLICATION_WINDOW_KIND")
        if auto_merge_cleanup is not None and not merge_window:
            _fail("PUBLICATION_WINDOW_KIND")
        if main_advanced_recovery and not recovery_window:
            _fail("PUBLICATION_WINDOW_KIND")
        # Validate using this real fence's own policy, checkout and lease store;
        # a serialized intent or caller-provided root cannot replace it.
        binding = (
            self.fence.validate_publication_merge_window(
                transaction, auto_merge_cleanup=auto_merge_cleanup,
            ) if auto_merge_cleanup is not None
            else self.fence.validate_publication_main_advanced_recovery_window(transaction)
            if main_advanced_recovery
            else self.fence.validate_publication_recovery_window(transaction) if recovery_window
            else self.fence.validate_publication_merge_window(transaction) if merge_window
            else self.fence.validate_publication_head_handoff(transaction) if head_handoff
            else self.fence.validate(
                transaction, exact_phase="LOCAL_MAIN_FF_PRE", require_candidate=True,
            )
        )
        replay = self.fence.replay(transaction)
        event = replay.events[-1]
        intent = event["payload"].get("local_publication_intent")
        if not isinstance(intent, dict) or any(
            checked[key] != expected for key, expected in {
                "publication_transaction_sha256": binding["transaction_sha256"],
                "local_publication_event_id": event["event_id"],
                "local_publication_intent_sha256": _canonical_sha256(intent),
                "lease_id": binding["lease_id"], "candidate_sha": binding["candidate_sha"],
                "expected_main_sha": binding["expected_main_sha"],
                "cwd": self.fence.project_root.as_posix(),
            }.items()
        ) or replay.transaction["actor"] != actor:
            _fail("PUBLICATION_ORIGINAL_BINDING")
        physical = super()._head(checked["lease_id"], actor)
        if physical.execution is None:
            _fail("PUBLICATION_FULL_REQUIRED")
        validate_execution(physical)
        full = full_execution_projection(physical.execution)
        # The execution subject is the checkout lease authority task, not the
        # user's publication task. The latter is bound by the original fence
        # and checkout intent; never collapse these two distinct identities.
        if (
            full["schema_version"] != "lease_execution.v4"
            or full["state"] != "RESULT_RECORDED" or full["result"]["status"] != "PASS"
            or checked["full_execution_sha256"] != _canonical_sha256(full)
            or any(checked[key] != full["request"][key] for key in (
                "lease_id", "manifest_sha256", "candidate_sha", "cwd", "host_id",
                "writer_epoch", "subject_task_id", "task_authority_sha256",
            ))
        ):
            _fail("PUBLICATION_FULL_BINDING")
        result_events = [row for row in replay.events if row["phase"] == "FORMAL_VALIDATION_RESULT"]
        if (len(result_events) != 1
                or result_events[0]["payload"].get("validation_status") != "PASS"):
            _fail("PUBLICATION_FULL_REQUIRED")
        self.fence._full_result_custody(
            replay, full, "PASS", result_events[0]["payload"]["evidence"],
        )
        return {"event_id": event["event_id"], "intent": _copy(intent), "full": full}

    def require_publication_worker(
        self, request: Mapping[str, Any], *, actor: str,
    ) -> dict[str, Any]:
        checked = _request(request)
        before = self._require_original_publication(checked, actor)
        witness = self._require_worker(
            checked, actor=actor, source_candidate=False, publication=True,
        )
        if self._require_original_publication(checked, actor) != before:
            _fail("PUBLICATION_ORIGINAL_CHANGED")
        return witness

    def require_head_handoff_worker(
        self, request: Mapping[str, Any], *, actor: str,
    ) -> dict[str, Any]:
        """Recheck original Full and native worker after only the recorded HEAD handoff."""
        checked = _request(request)
        before = self._require_original_publication(checked, actor, head_handoff=True)
        witness = self._require_worker(
            checked, actor=actor, source_candidate=False, publication=True,
        )
        current = self._head(checked["lease_id"], actor).execution
        if (current is None
                or witness["worker_process"] != current["git_launch"]["worker_process"]
                or self._require_original_publication(checked, actor, head_handoff=True) != before):
            _fail("PUBLICATION_HEAD_HANDOFF_WORKER_CHANGED")
        return witness

    def record_checkout_plan(
        self, request: Mapping[str, Any], *, actor: str,
    ) -> dict[str, Any]:
        """Append once under original Full/Job custody; grants no checkout capability."""
        from ai_trading_system.platform.architecture.workflow_integration import (
            prepare_local_publication_checkout_plan,
        )

        checked = _request(request)
        worker = self.require_publication_worker(checked, actor=actor)
        self.fence._require_clean_candidate()
        head = self._head(checked["lease_id"], actor)
        if head.execution is None or head.execution["state"] != "RUNNING":
            _fail("PUBLICATION_CHECKOUT_PLAN_STATE")
        if "main_preparation" in head.execution:
            _fail("PUBLICATION_CHECKOUT_PLAN_TOO_LATE")
        plan = prepare_local_publication_checkout_plan(self.fence.project_root, checked)
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            current = self._head(checked["lease_id"], actor)
            if (current.execution != head.execution
                    or self.require_publication_worker(checked, actor=actor) != worker):
                _fail("PUBLICATION_CHECKOUT_PLAN_CHANGED")
            self.fence._require_clean_candidate()
            if prepare_local_publication_checkout_plan(self.fence.project_root, checked) != plan:
                _fail("PUBLICATION_CHECKOUT_PLAN_CHANGED")
            value = _copy(current.execution)
            prior = value.get("checkout_plan")
            if prior is not None:
                if prior["plan"] != plan or prior["worker_process"] != worker["worker_process"]:
                    _fail("PUBLICATION_CHECKOUT_PLAN_CHANGED")
                return {"status": "REPLAY_ONLY", "binding": _copy(prior),
                        "dispatch_allowed": False, "publication_allowed": False}
            record = {
                "schema_version": "workflow_publication_checkout_plan_binding.v1",
                "request_sha256": value["request_sha256"], "plan": plan,
                "worker_process": worker["worker_process"], "observed_at": instant.isoformat(),
            }
            value["checkout_plan"] = record
            self._append(current, value, instant)
            return {"status": "RECORDED", "binding": _copy(record),
                    "dispatch_allowed": False, "publication_allowed": False}

    def record_hook_capsule_definition(
        self, request: Mapping[str, Any], *, actor: str,
    ) -> dict[str, Any]:
        """Record the original attempt's definition before any hook file is created."""
        from ai_trading_system.platform.architecture.workflow_integration import (
            define_local_publication_hook_capsule,
            prepare_local_publication_checkout_plan,
        )

        checked = _request(request)
        worker = self.require_publication_worker(checked, actor=actor)
        self.fence._require_clean_candidate()
        head = self._head(checked["lease_id"], actor)
        if (head.execution is None or head.execution["state"] != "RUNNING"
                or "checkout_plan" not in head.execution or "main_preparation" in head.execution):
            _fail("PUBLICATION_HOOK_CAPSULE_STATE")
        plan = head.execution["checkout_plan"]
        if (plan["worker_process"] != worker["worker_process"]
                or prepare_local_publication_checkout_plan(self.fence.project_root, checked)
                != plan["plan"]):
            _fail("PUBLICATION_HOOK_CAPSULE_CHANGED")
        definition = define_local_publication_hook_capsule(
            checked, plan["plan"], actor=actor, policy_path=self.fence.policy_path,
        )
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            current = self._head(checked["lease_id"], actor)
            if (current.execution != head.execution
                    or self.require_publication_worker(checked, actor=actor) != worker):
                _fail("PUBLICATION_HOOK_CAPSULE_CHANGED")
            self.fence._require_clean_candidate()
            if (prepare_local_publication_checkout_plan(self.fence.project_root, checked)
                    != plan["plan"]):
                _fail("PUBLICATION_HOOK_CAPSULE_CHANGED")
            value = _copy(current.execution)
            prior = value.get("hook_capsule")
            if prior is not None:
                if prior["definition"] != definition or prior["worker_process"] != worker[
                    "worker_process"
                ]:
                    _fail("PUBLICATION_HOOK_CAPSULE_CHANGED")
                return {"status": "REPLAY_ONLY", "binding": _copy(prior),
                        "dispatch_allowed": False, "publication_allowed": False}
            record = {
                "schema_version": "workflow_publication_hook_capsule.v1",
                "request_sha256": value["request_sha256"], "definition": definition,
                "worker_process": worker["worker_process"], "observed_at": instant.isoformat(),
                "objects": [], "ready": None,
            }
            value["hook_capsule"] = record
            self._append(current, value, instant)
            return {"status": "RECORDED", "binding": _copy(record),
                    "dispatch_allowed": False, "publication_allowed": False}

    def _record_hook_created_object(
        self, request: Mapping[str, Any], descriptor: int, *, actor: str,
        parent_identities: Mapping[str, tuple[int, int]],
    ) -> dict[str, Any]:
        """Called only from the original recoverable create while delete-on-close is armed."""
        checked = _request(request)
        worker = self.require_publication_worker(checked, actor=actor)
        head = self._head(checked["lease_id"], actor)
        if (head.execution is None or head.execution["state"] != "RUNNING"
                or "hook_capsule" not in head.execution or "main_preparation" in head.execution):
            _fail("PUBLICATION_HOOK_CREATION_STATE")
        capsule = head.execution["hook_capsule"]
        definition, objects = capsule["definition"], capsule["objects"]
        if (capsule["ready"] is not None or len(objects) >= 3
                or capsule["worker_process"] != worker["worker_process"]):
            _fail("PUBLICATION_HOOK_CREATION_STATE")
        expected = [("directory", definition["directory"], None), *[
            ("file", row["path"], row["sha256"]) for row in definition["files"]
        ]]
        kind, relative, target_sha = expected[len(objects)]
        parts = Path(relative).parts
        names = {"/".join(parts[:index]) for index in range(1, len(parts))}
        if set(parent_identities) != names or any(
            not isinstance(pair, tuple) or len(pair) != 2
            or any(type(item) is not int or item < 0 for item in pair)
            for pair in parent_identities.values()
        ):
            _fail("PUBLICATION_HOOK_CREATION_PARENTS")
        root = self.fence.project_root
        root_identity = head.execution["checkout_plan"]["plan"]["topology"][
            "candidate_checkout"
        ]["root"]["identity"]
        identity = _publication_created_object_identity(descriptor, root / relative, kind)

        def namespace() -> None:
            info = directory_identity(root)
            if [info["device"], info["file_id"]] != root_identity:
                _fail("PUBLICATION_HOOK_CREATION_ROOT_CHANGED")
            for name, wanted in parent_identities.items():
                info = directory_identity(root / name)
                if (info["device"], info["file_id"]) != wanted:
                    _fail("PUBLICATION_HOOK_CREATION_NAMESPACE_CHANGED")

        namespace()
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            current = self._head(checked["lease_id"], actor)
            if (current.execution != head.execution
                    or self.require_publication_worker(checked, actor=actor) != worker
                    or _publication_created_object_identity(descriptor, root / relative, kind)
                    != identity):
                _fail("PUBLICATION_HOOK_CREATION_CHANGED")
            self.fence._require_clean_candidate()
            namespace()
            value = _copy(current.execution)
            record = {
                "schema_version": "workflow_publication_hook_created_object.v1",
                "request_sha256": capsule["request_sha256"],
                "definition_sha256": definition["definition_sha256"], "kind": kind,
                "path": relative, "root_identity": root_identity,
                "parent_identities": {name: list(pair) for name, pair in parent_identities.items()},
                "file_identity": identity, "worker_process": worker["worker_process"],
                "observed_at": instant.isoformat(), "target_sha256": target_sha,
            }
            value["hook_capsule"]["objects"].append(record)
            self._append(current, value, instant)
            return dict(_copy(record))

    def materialize_hook_capsule(
        self, request: Mapping[str, Any], *, actor: str,
    ) -> dict[str, Any]:
        """Create the three fixed objects, recording each held identity before keeping it.

        Partial attempts are never overwritten or silently retried. This returns
        creation evidence, not readiness, file custody, or a Git resume capability.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            bounded_regular_bytes,
            create_bound_recoverable_directory,
            create_bound_recoverable_file,
        )

        checked = _request(request)
        recorded = self.record_hook_capsule_definition(checked, actor=actor)
        capsule = recorded["binding"]
        if capsule["objects"] or capsule["ready"] is not None:
            _fail("PUBLICATION_HOOK_CAPSULE_ALREADY_CREATED")
        root = self.fence.project_root
        head = self._head(checked["lease_id"], actor)
        assert head.execution is not None
        root_pair = head.execution["checkout_plan"]["plan"]["topology"]["candidate_checkout"][
            "root"
        ]["identity"]
        root_identity = (root_pair[0], root_pair[1])
        definition = capsule["definition"]
        parents: dict[str, tuple[int, int]] = {}
        parts = Path(definition["directory"]).parts
        for position in range(1, len(parts)):
            name = "/".join(parts[:position])
            info = directory_identity(root / name)
            parents[name] = (info["device"], info["file_id"])
        created: list[dict[str, Any]] = []

        def record(descriptor: int) -> None:
            created.append(self._record_hook_created_object(
                checked, descriptor, actor=actor, parent_identities=parents,
            ))

        create_bound_recoverable_directory(
            root, definition["directory"], record_created=record,
            expected_root_identity=root_identity, expected_parent_identities=parents,
        )
        directory_pair = created[0]["file_identity"]
        parents[definition["directory"]] = (directory_pair[0], directory_pair[1])
        for row in definition["files"]:
            raw = bytes.fromhex(row["bytes_hex"])
            create_bound_recoverable_file(
                root, row["path"], raw, record_created=record,
                expected_root_identity=root_identity, expected_parent_identities=parents,
            )
            pair = created[-1]["file_identity"]
            if bounded_regular_bytes(
                root / row["path"], expected_identity=(pair[0], pair[1]),
            ) != raw:
                _fail("PUBLICATION_HOOK_CREATION_BYTES")
        current = self._head(checked["lease_id"], actor)
        assert current.execution is not None
        if current.execution["hook_capsule"]["objects"] != created:
            _fail("PUBLICATION_HOOK_CREATION_CHANGED")
        return {"status": "CREATED_NOT_READY", "binding": _copy(current.execution["hook_capsule"]),
                "dispatch_allowed": False, "publication_allowed": False, "resume_allowed": False}

    @contextmanager
    def hold_hook_capsule_inputs(
        self, request: Mapping[str, Any], *, actor: str,
    ) -> Iterator[tuple[dict[str, Any], tuple[_BoundReadFileCustody, ...]]]:
        """Retain original Full inputs and fixed hooks under the actual worker.

        This neither appends readiness nor grants Git dispatch. The returned
        snapshot describes these live objects only during this context; an
        eventual ready transition must recheck the same outer execution before
        appending once, and Git must inherit the actual native handles.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            bounded_regular_bytes,
            canonical_digest,
            hold_bound_read_file,
        )
        from ai_trading_system.platform.architecture.workflow_execution import (
            hold_acceptance_runtime_identity,
        )

        checked = _request(request)
        worker = self.require_publication_worker(checked, actor=actor)
        self.fence._require_clean_candidate()
        head = self._head(checked["lease_id"], actor)
        value = head.execution
        if (value is None or value["state"] != "RUNNING" or "main_preparation" in value
                or "hook_capsule" not in value or len(value["hook_capsule"]["objects"]) != 3
                or value["hook_capsule"]["ready"] is not None):
            _fail("PUBLICATION_HOOK_INPUT_STATE")
        capsule = value["hook_capsule"]
        if capsule["worker_process"] != worker["worker_process"]:
            _fail("PUBLICATION_HOOK_INPUT_WORKER")
        physical = super()._head(checked["lease_id"], actor)
        assert physical.execution is not None
        identity_sha = full_execution_projection(physical.execution)["request"][
            "validation_identity_sha256"
        ]
        root = self.fence.project_root
        root_pair = value["checkout_plan"]["plan"]["topology"]["candidate_checkout"][
            "root"
        ]["identity"]
        roots = {root: (root_pair[0], root_pair[1])}
        identities: dict[Path, tuple[int, int]] = dict(roots)
        transaction = Path(checked["publication_transaction_path"])
        policy = self.fence.policy_path.absolute()
        if not policy.is_relative_to(root):
            info = directory_identity(policy.parent)
            roots[policy.parent] = (info["device"], info["file_id"])
            identities[policy.parent] = roots[policy.parent]

        def directory(path: Path) -> tuple[int, int]:
            if path not in identities:
                info = directory_identity(path)
                identities[path] = (info["device"], info["file_id"])
            return identities[path]

        with ExitStack() as stack:
            runtime, runtime_files = stack.enter_context(hold_acceptance_runtime_identity())
            # The original inspector already validates and captures every
            # candidate runner source, checkout input and retained Full/readiness
            # dependency. Do not replace it with another source inventory.
            profile = self.fence._prepare_current_full_profile(transaction, actor=actor)
            inspection = json.loads(profile[3])
            files = list(runtime_files)
            original_identities: list[Any] = []

            def retain(
                path: Path, raw: bytes, *, creation: Mapping[str, Any] | None = None,
                budget: int = 16 * 1024 * 1024,
            ) -> None:
                target_root = root if path.is_relative_to(root) else policy.parent
                if (target_root not in roots or (target_root != root and path != policy)
                        or ".." in path.parts):
                    _fail("PUBLICATION_HOOK_INPUT_PATH")
                relative = path.relative_to(target_root)
                parents = {
                    "/".join(relative.parts[:position]): directory(
                        target_root.joinpath(*relative.parts[:position])
                    ) for position in range(1, len(relative.parts))
                }
                info = path.stat()
                pair = (info.st_dev, info.st_ino)
                if creation is not None:
                    pair = (creation["file_identity"][0], creation["file_identity"][1])
                    parents = {
                        key: (item[0], item[1])
                        for key, item in creation["parent_identities"].items()
                    }
                files.append(stack.enter_context(hold_bound_read_file(
                    target_root, relative.as_posix(), expected=raw, expected_identity=pair,
                    expected_root_identity=roots[target_root], expected_parent_identities=parents,
                    budget=budget,
                )))

            for row, creation in zip(capsule["definition"]["files"], capsule["objects"][1:],
                                     strict=True):
                retain(root / row["path"], bytes.fromhex(row["bytes_hex"]), creation=creation)
            for row in inspection["captures"]:
                path = Path(row["path"])
                if not path.is_relative_to(root) or ".." in path.parts:
                    _fail("PUBLICATION_HOOK_INPUT_PATH")
                raw = bounded_regular_bytes(path, budget=PUBLICATION_CAPTURE_BUDGET_BYTES)
                if (len(raw) != row["size_bytes"]
                        or hashlib.sha256(raw).hexdigest() != row["sha256"]):
                    _fail("PUBLICATION_HOOK_INPUT_CHANGED")
                retain(path, raw, budget=PUBLICATION_CAPTURE_BUDGET_BYTES)
                if (path.name == "execution_validation_identity.json"
                        and row["sha256"] == identity_sha):
                    original_identities.append(json.loads(raw))
            # Original immutable transaction and selected policy are hook inputs
            # too. Their semantic/raw identities are rechecked by the same fence
            # below, while these exact native objects remain held.
            for path in (transaction, policy):
                retain(path, bounded_regular_bytes(path))
            instant = datetime.now(UTC)
            with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
                current = self._head(checked["lease_id"], actor)
                if (current.execution != value
                        or self.require_publication_worker(checked, actor=actor) != worker):
                    _fail("PUBLICATION_HOOK_INPUT_CHANGED")
                self.fence._require_clean_candidate()
                physical = super()._head(checked["lease_id"], actor)
                self.fence._recheck_full_profile(
                    self.fence.replay(transaction), physical.execution, profile,
                    phase="LOCAL_MAIN_FF_PRE",
                )
                if (len(original_identities) != 1
                        or not isinstance(original_identities[0], dict)
                        or not isinstance(original_identities[0].get("runtime"), dict)
                        or {key: item for key, item in original_identities[0]["runtime"].items()
                            if key != "environment_sha256"}
                        != {key: item for key, item in runtime.items()
                            if key != "environment_sha256"}):
                    _fail("PUBLICATION_HOOK_INPUT_ORIGINAL_RUNTIME")
                snapshot = {
                    "schema_version": "workflow_publication_live_inputs.v1",
                    "request_sha256": canonical_digest(checked),
                    "definition_sha256": capsule["definition"]["definition_sha256"],
                    "worker_process": worker["worker_process"], "observed_at": instant.isoformat(),
                    "profile_inspection": inspection, "runtime_identity": runtime,
                    "runtime_file_count": len(runtime_files),
                    "read_file_custodies": [item.binding() for item in files],
                    "dispatch_allowed": False, "publication_allowed": False,
                    "resume_allowed": False,
                }
            yield snapshot, tuple(files)

    @contextmanager
    def prepare_hook_capsule_ready(
        self, request: Mapping[str, Any], *, actor: str,
    ) -> Iterator[tuple[dict[str, Any], tuple[_BoundReadFileCustody, ...]]]:
        """Append readiness once from internal live custody, not caller-provided proofs.

        The returned files remain held only through this context. A serialized
        ready record cannot recreate them or permit Git dispatch after exit.
        """
        checked = _request(request)
        with self.hold_hook_capsule_inputs(checked, actor=actor) as (inputs, files):
            instant = datetime.now(UTC)
            with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
                head = self._head(checked["lease_id"], actor)
                value = _copy(head.execution)
                if (value is None or value["state"] != "RUNNING" or "main_preparation" in value
                        or value["hook_capsule"]["ready"] is not None
                        or self.require_publication_worker(checked, actor=actor)["worker_process"]
                        != inputs["worker_process"]
                        or [item.binding() for item in files] != inputs["read_file_custodies"]):
                    _fail("PUBLICATION_HOOK_READY_LIVE_INPUTS_CHANGED")
                self.fence._require_clean_candidate()
                physical = super()._head(checked["lease_id"], actor)
                inspection = inputs["profile_inspection"]
                preparation = (
                    inspection["transaction_sha256"], inspection["head_event_id"],
                    "LOCAL_MAIN_FF_PRE",
                    json.dumps(inspection, sort_keys=True, separators=(",", ":")).encode(),
                )
                self.fence._recheck_full_profile(
                    self.fence.replay(Path(checked["publication_transaction_path"])),
                    physical.execution, preparation, phase="LOCAL_MAIN_FF_PRE",
                )
                record = {
                    "schema_version": "workflow_publication_hook_ready.v1",
                    "request_sha256": value["request_sha256"], "inputs": _copy(inputs),
                    "recorded_at": instant.isoformat(),
                }
                value["hook_capsule"]["ready"] = record
                self._append(head, value, instant)
            yield _copy(record), files

    @contextmanager
    def prepare_git_launch(
        self, request: Mapping[str, Any], ready_files: Sequence[object], *, actor: str,
    ) -> Iterator[tuple[InheritedJobChild, tuple[_BoundReadFileCustody, ...]]]:
        """Create and bind the actual suspended merge while original inputs stay held.

        This composes the existing ready context and original Job, not a second
        launcher. The caller must perform the governed HEAD/resume steps inside
        this context. Exit closes this exact child and its installed-image holds.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            _BoundReadFileCustody,
            bounded_regular_bytes,
            hold_bound_read_file,
        )
        from ai_trading_system.platform.architecture.workflow_execution import InheritedJobChild

        if (not isinstance(ready_files, (list, tuple))
                or any(not isinstance(item, _BoundReadFileCustody) for item in ready_files)):
            _fail("PUBLICATION_GIT_LAUNCH_LIVE_OBJECTS_REQUIRED")
        checked = _request(request)
        self.require_publication_worker(checked, actor=actor)
        value = self._head(checked["lease_id"], actor).execution
        if (value is None or "git_launch" in value or "main_preparation" in value
                or not value.get("hook_capsule", {}).get("ready")):
            _fail("PUBLICATION_GIT_LAUNCH_STATE")
        if [item.binding() for item in ready_files] != value["hook_capsule"]["ready"][
            "inputs"
        ]["read_file_custodies"]:
            _fail("PUBLICATION_GIT_LAUNCH_INPUTS_CHANGED")
        executable = shutil.which("git")
        if executable is None:
            _fail("PUBLICATION_GIT_UNAVAILABLE")
        launcher = Path(executable).resolve()
        if launcher.name.casefold() != "git.exe" or launcher.parent.name.casefold() not in {
            "cmd", "bin",
        }:
            _fail("PUBLICATION_GIT_LAUNCH_COMMAND")
        root = _git_installation_root(launcher)
        root_info = root.lstat()
        root_identity = (root_info.st_dev, root_info.st_ino)
        environment = {key: item for key, item in os.environ.items()
                       if not key.upper().startswith("GIT_")}
        environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                           GIT_OPTIONAL_LOCKS="0")
        with ExitStack() as stack:
            files: list[_BoundReadFileCustody] = list(ready_files)
            for path in (launcher, root / "mingw64/bin/git.exe", root / "usr/bin/sh.exe"):
                relative = path.relative_to(root)
                parents = {}
                for index in range(1, len(relative.parts)):
                    parent = Path(*relative.parts[:index])
                    info = (root / parent).lstat()
                    parents[parent.as_posix()] = (info.st_dev, info.st_ino)
                info = path.lstat()
                identity = (info.st_dev, info.st_ino)
                raw = bounded_regular_bytes(
                    path, expected_identity=identity, expected_link_count=info.st_nlink,
                )
                files.append(stack.enter_context(hold_bound_read_file(
                    root, relative.as_posix(), expected=raw, expected_identity=identity,
                    expected_root_identity=root_identity, expected_parent_identities=parents,
                    expected_link_count=info.st_nlink,
                )))
            child = stack.enter_context(InheritedJobChild.create(
                argv=[launcher.as_posix(), "-c", "core.hooksPath=" + (
                    Path(checked["cwd"]) / value["hook_capsule"]["definition"]["directory"]
                ).as_posix(), "-c", "maintenance.auto=false",
                *value["checkout_plan"]["plan"]["merge_argv_tail"]],
                cwd=Path(checked["cwd"]), environment=environment,
                stdout_path=(Path(checked["stdout_path"]).parent
                             / ("publication-merge-" + value["request_sha256"] + ".stdout")),
                job_name=checked["job_name"], file_custodies=files,
            ))
            self.record_git_launch(checked, child, files, actor=actor)
            yield child, tuple(files)

    @contextmanager
    def switch_publication_heads(
        self, request: Mapping[str, Any], child: object, files: Sequence[object], *,
        actor: str, authorize_peer_head_handoff: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Persist original HEAD intent, then apply exact native before/after writes.

        Administrative directory custody stays held through the caller's body.
        No Git resume, ref advance or worker PASS is granted here. Failure keeps
        the original effect intent and lease; unchanged recovery cannot release
        a partially switched checkout. The explicit peer option authorizes only
        the original plan's HEAD, never its index or private working files.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            _BoundReadFileCustody,
            apply_bound_file,
            canonical_digest,
            hold_bound_directory,
        )
        from ai_trading_system.platform.architecture.workflow_execution import InheritedJobChild
        from ai_trading_system.platform.architecture.workflow_integration import (
            _local_publication_metadata,
            inspect_publication_switched_heads,
            prepare_local_publication_checkout_plan,
        )

        if (not isinstance(child, InheritedJobChild) or not isinstance(files, (tuple, list))
                or any(not isinstance(item, _BoundReadFileCustody) for item in files)
                or type(authorize_peer_head_handoff) is not bool):
            _fail("PUBLICATION_GIT_LAUNCH_LIVE_OBJECTS_REQUIRED")
        checked = _request(request)
        worker = self.require_publication_worker(checked, actor=actor)
        head = self._head(checked["lease_id"], actor)
        value = _copy(head.execution)
        if (value is None or value["state"] != "RUNNING" or "git_launch" not in value
                or "checkout_effect" in value):
            _fail("PUBLICATION_CHECKOUT_EFFECT_STATE")
        plan = value["checkout_plan"]["plan"]
        if plan["peer_handoff_required"] and not authorize_peer_head_handoff:
            _fail("PUBLICATION_PEER_HEAD_HANDOFF_APPROVAL_REQUIRED")
        native = child.pre_resume_binding()
        bindings = [item.binding() for item in files]
        if (canonical_digest(native) != value["git_launch"]["pre_resume_sha256"]
                or native["read_file_custodies"] != bindings
                or worker["worker_process"] != value["git_launch"]["worker_process"]):
            _fail("PUBLICATION_GIT_LAUNCH_NATIVE_BINDING")
        topology = plan["topology"]
        checkouts = [topology["candidate_checkout"]]
        if topology["main_checkout"] is not None:
            checkouts.append(topology["main_checkout"])
        # Pin the original administrative directory objects before the intent;
        # leaf writes remain exact physical identity + bytes comparisons.
        with ExitStack() as stack:
            held_paths: set[str] = set()
            for checkout in checkouts:
                for key in ("root", "gitdir", "common"):
                    directory = checkout[key]
                    path = Path(directory["path"])
                    if path.as_posix() in held_paths:
                        continue
                    parent = path.parent.lstat()
                    stack.enter_context(hold_bound_directory(
                        path.parent, path.name, expected_identity=tuple(directory["identity"]),
                        expected_root_identity=(parent.st_dev, parent.st_ino),
                        expected_parent_identities={},
                        allow_child_updates=True,
                    ))
                    held_paths.add(path.as_posix())
            instant = datetime.now(UTC)
            with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
                current = self._head(checked["lease_id"], actor)
                if (current.execution != value
                        or self.require_publication_worker(checked, actor=actor) != worker
                        or prepare_local_publication_checkout_plan(self.fence.project_root, checked)
                        != plan or child.pre_resume_binding() != native
                        or [item.binding() for item in files] != bindings):
                    _fail("PUBLICATION_CHECKOUT_EFFECT_CHANGED")
                self.fence._require_clean_candidate()
                value["checkout_effect"] = {
                    "schema_version": "workflow_publication_checkout_effect.v1",
                    "request_sha256": value["request_sha256"],
                    "git_launch_sha256": canonical_digest(value["git_launch"]),
                    "checkout_plan_sha256": plan["plan_sha256"],
                    "worker_process": worker["worker_process"], "state": "HEAD_HANDOFF_INTENT",
                    "peer_handoff_authorized": plan["peer_handoff_required"],
                    "started_at": instant.isoformat(), "completed_at": None,
                    "head_observations": [],
                }
                current = self._append(current, value, instant)
                observations = []
                for transition in plan["head_transitions"]:
                    before = transition["before"]
                    checkout = (topology["main_checkout"] if transition["role"] == "peer_head"
                                else topology["candidate_checkout"])
                    if child.pre_resume_binding() != native:
                        _fail("PUBLICATION_GIT_LAUNCH_NATIVE_BINDING")
                    apply_bound_file(
                        Path(checkout["gitdir"]["path"]), "HEAD",
                        bytes.fromhex(before["bytes_hex"]), bytes.fromhex(transition["after_hex"]),
                        expected_identity=tuple(before["identity"]),
                        expected_root_identity=tuple(checkout["gitdir"]["identity"]),
                        expected_parent_identities={},
                    )
                    observations.append({"role": transition["role"], **_local_publication_metadata(
                        Path(before["path"]), contents=True,
                    )})
                if (child.pre_resume_binding() != native
                        or [item.binding() for item in files] != bindings):
                    _fail("PUBLICATION_GIT_LAUNCH_NATIVE_BINDING")
                value["checkout_effect"].update(
                    state="HEADS_SWITCHED", completed_at=datetime.now(UTC).isoformat(),
                    head_observations=observations,
                )
                _validate_publication_checkout_effect(value)
                self._append(current, value, datetime.now(UTC))
                inspect_publication_switched_heads(self.fence.project_root, checked, plan)
                self.require_head_handoff_worker(checked, actor=actor)
            yield {"status": "HEADS_SWITCHED", "dispatch_allowed": False,
                   "resume_allowed": False, "publication_allowed": False}

    def resume_publication_git(
        self, request: Mapping[str, Any], child: object, files: Sequence[object], *, actor: str,
    ) -> dict[str, Any]:
        """Persist the original resume intent before the one native ResumeThread."""
        from ai_trading_system.platform.architecture.workflow_contract import (
            _BoundReadFileCustody,
            canonical_digest,
        )
        from ai_trading_system.platform.architecture.workflow_execution import InheritedJobChild

        if (not isinstance(child, InheritedJobChild) or not isinstance(files, (tuple, list))
                or any(not isinstance(item, _BoundReadFileCustody) for item in files)):
            _fail("PUBLICATION_GIT_LAUNCH_LIVE_OBJECTS_REQUIRED")
        checked = _request(request)
        worker = self.require_head_handoff_worker(checked, actor=actor)
        head = self._head(checked["lease_id"], actor)
        value = _copy(head.execution)
        if value is None or "git_merge" in value:
            _fail("PUBLICATION_GIT_MERGE_ALREADY_REQUESTED")
        native = child.pre_resume_binding()
        bindings = [item.binding() for item in files]
        if (canonical_digest(native) != value["git_launch"]["pre_resume_sha256"]
                or native["read_file_custodies"] != bindings):
            _fail("PUBLICATION_GIT_LAUNCH_NATIVE_BINDING")
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            current = self._head(checked["lease_id"], actor)
            if (current.execution != value
                    or self.require_head_handoff_worker(checked, actor=actor) != worker
                    or child.pre_resume_binding() != native
                    or [item.binding() for item in files] != bindings):
                _fail("PUBLICATION_GIT_MERGE_CHANGED")
            value["git_merge"] = {
                "schema_version": "workflow_publication_git_merge.v1",
                "request_sha256": value["request_sha256"],
                "git_launch_sha256": canonical_digest(value["git_launch"]),
                "checkout_effect_sha256": canonical_digest(value["checkout_effect"]),
                "worker_process": worker["worker_process"], "resume_intent_at": instant.isoformat(),
                "hooks": [], "exit": None,
            }
            self._append(current, value, instant)
            child.resume()  # No process wait inside the original short arbiter.
        return {"status": "GIT_RESUME_REQUESTED", "dispatch_allowed": False,
                "publication_allowed": False}

    def record_publication_hook(
        self, transaction: Path, *, actor: str, request_sha: str, kind: str, stage: str,
        updates: bytes,
    ) -> dict[str, Any]:
        """The fixed CLI accepts only a real descendant of the original Git handle."""
        from ai_trading_system.platform.architecture.workflow_execution import (
            hold_contained_ancestor,
            observe_process,
        )
        from ai_trading_system.platform.architecture.workflow_integration import (
            _local_publication_metadata,
            classify_publication_prepared_reference_update,
            observe_publication_auto_merge_lock,
        )

        if type(updates) is not bytes or len(updates) > 256:
            _fail("PUBLICATION_GIT_MERGE_UPDATES")
        replay = self.fence.replay(transaction)
        if replay.status != "PASS" or replay.transaction["actor"] != actor:
            _fail("PUBLICATION_GIT_MERGE_TRANSACTION")
        head = self._head(str(replay.transaction["lease_id"]), actor)
        value = _copy(head.execution)
        if (value is None or value["state"] != "RUNNING" or "git_merge" not in value
                or value["git_merge"]["exit"] is not None
                or value["request_sha256"] != request_sha):
            _fail("PUBLICATION_GIT_MERGE_HOOK_STATE")
        checked = _request(value["request"])
        launch, plan = value["git_launch"], value["checkout_plan"]["plan"]
        reference = None
        if kind == "reference-transaction" and stage in {"prepared", "committed", "aborted"}:
            reference = classify_publication_prepared_reference_update(plan, checked, updates)
        elif kind != "post-merge" or stage != "0" or updates:
            _fail("PUBLICATION_GIT_MERGE_HOOK_KIND")
        with hold_contained_ancestor(checked["job_name"], launch["process"]) as chain:
            self._require_worker(checked, actor=actor, source_candidate=False, publication=True)
            if observe_process(**launch["worker_process"])["state"] != "RUNNING":
                _fail("PUBLICATION_GIT_MERGE_WORKER_EXITED")
            cleanup_lock = (observe_publication_auto_merge_lock(plan, value["git_merge"], stage)
                            if reference == "AUTO_MERGE" else None)
            cleanup_window = ({"stage": stage, "metadata": cleanup_lock}
                              if cleanup_lock is not None else None)
            original = self._require_original_publication(
                checked, actor, merge_window=True, auto_merge_cleanup=cleanup_window,
            )
            prepared = None
            if stage == "prepared" and reference in {"ORIG_HEAD", "FAST_FORWARD"}:
                checkout = plan["topology"]["candidate_checkout"]
                path = (Path(checkout["gitdir"]["path"]) / "ORIG_HEAD.lock"
                        if reference == "ORIG_HEAD"
                        else Path(checkout["common"]["path"]) / "refs/heads/main.lock")
                prepared = _local_publication_metadata(path, contents=True)
            instant = datetime.now(UTC)
            record = {"kind": kind, "stage": stage, "reference_kind": reference,
                      "updates_hex": updates.hex(), "process_chain": list(chain),
                      "observed_at": instant.isoformat(), "prepared_file": prepared}
            if reference == "AUTO_MERGE":
                record["cleanup_lock"] = cleanup_lock
            with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
                current = self._head(checked["lease_id"], actor)
                if (current.execution != value
                        or self._require_original_publication(
                            checked, actor, merge_window=True, auto_merge_cleanup=cleanup_window,
                        )
                        != original
                        or observe_process(**launch["worker_process"])["state"] != "RUNNING"
                        or (prepared is not None and _local_publication_metadata(
                            Path(prepared["path"]), contents=True,
                        ) != prepared)):
                    _fail("PUBLICATION_GIT_MERGE_HOOK_CHANGED")
                if reference == "AUTO_MERGE" and observe_publication_auto_merge_lock(
                    plan, value["git_merge"], stage,
                ) != cleanup_lock:
                    _fail("PUBLICATION_GIT_MERGE_HOOK_CHANGED")
                value["git_merge"]["hooks"].append(record)
                _validate_publication_git_merge(value)
                self._append(current, value, instant)
        return {"status": "ORIGINAL_GIT_HOOK_RECORDED", "kind": kind, "stage": stage,
                "dispatch_allowed": False, "publication_allowed": False}

    def record_publication_git_exit(
        self, request: Mapping[str, Any], child: object, *, actor: str,
    ) -> dict[str, Any]:
        """Record the original direct child's native exit, not whole-Job completion."""
        from ai_trading_system.platform.architecture.workflow_execution import InheritedJobChild
        from ai_trading_system.platform.architecture.workflow_integration import (
            inspect_publication_merge_window,
        )

        if not isinstance(child, InheritedJobChild):
            _fail("PUBLICATION_GIT_LAUNCH_LIVE_OBJECTS_REQUIRED")
        checked = _request(request)
        worker = self._require_worker(
            checked, actor=actor, source_candidate=False, publication=True,
        )
        head = self._head(checked["lease_id"], actor)
        value = _copy(head.execution)
        if value is None or "git_merge" not in value or value["git_merge"]["exit"] is not None:
            _fail("PUBLICATION_GIT_MERGE_EXIT_STATE")
        identity = child.identity()
        process = {"pid": identity["pid"], "creation_time": identity["creation_time"]}
        code = child.poll()
        if (code is None or process != value["git_launch"]["process"]
                or worker["worker_process"] != value["git_launch"]["worker_process"]):
            _fail("PUBLICATION_GIT_MERGE_EXIT_NATIVE")
        original = self._require_original_publication(checked, actor, merge_window=True)
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            current = self._head(checked["lease_id"], actor)
            if (current.execution != value or child.identity() != identity or child.poll() != code
                    or self._require_original_publication(checked, actor, merge_window=True)
                    != original):
                _fail("PUBLICATION_GIT_MERGE_EXIT_CHANGED")
            value["git_merge"]["exit"] = {
                "process": process, "worker_process": worker["worker_process"],
                "returncode": code, "observed_at": instant.isoformat(),
            }
            inspect_publication_merge_window(
                self.fence.project_root, checked, value["checkout_plan"]["plan"],
                value["git_merge"],
            )
            self._append(current, value, instant)
        return {"status": "ORIGINAL_GIT_EXIT_RECORDED", "returncode": code,
                "dispatch_allowed": False, "publication_allowed": False}

    def record_git_launch(
        self, request: Mapping[str, Any], child: object,
        files: Sequence[object], *, actor: str,
    ) -> dict[str, Any]:
        """Bind the original suspended ff-only child before any checkout effect.

        Only live owner-held child/file objects are accepted. This records an
        observation in the original store; it neither resumes nor returns a new
        process capability. The later effect-window operation must recheck the
        original handles and persist its separate resume intent.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            _BoundReadFileCustody,
            canonical_digest,
        )
        from ai_trading_system.platform.architecture.workflow_execution import (
            InheritedJobChild,
            execution_environment_sha256,
        )

        if (not isinstance(child, InheritedJobChild)
                or not isinstance(files, (tuple, list))
                or any(not isinstance(item, _BoundReadFileCustody) for item in files)):
            _fail("PUBLICATION_GIT_LAUNCH_LIVE_OBJECTS_REQUIRED")
        checked = _request(request)
        worker = self.require_publication_worker(checked, actor=actor)
        head = self._head(checked["lease_id"], actor)
        value = _copy(head.execution)
        if (value is None or value["state"] != "RUNNING" or "git_launch" in value
                or "main_preparation" in value or not value.get("hook_capsule", {}).get("ready")):
            _fail("PUBLICATION_GIT_LAUNCH_STATE")
        ready = value["hook_capsule"]["ready"]
        original_files = ready["inputs"]["read_file_custodies"]
        if len(files) != len(original_files) + 3:
            _fail("PUBLICATION_GIT_LAUNCH_FILES")
        bindings = [item.binding() for item in files]
        if bindings[:-3] != original_files:
            _fail("PUBLICATION_GIT_LAUNCH_INPUTS_CHANGED")
        native = child.pre_resume_binding()
        executable = shutil.which("git")
        if executable is None:
            _fail("PUBLICATION_GIT_UNAVAILABLE")
        environment = {key: item for key, item in os.environ.items()
                       if not key.upper().startswith("GIT_")}
        environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                           GIT_OPTIONAL_LOCKS="0")
        expected_launch = {
            "argv": [Path(executable).resolve().as_posix(), "-c", "core.hooksPath=" + (
                Path(checked["cwd"]) / value["hook_capsule"]["definition"]["directory"]
            ).as_posix(), "-c", "maintenance.auto=false",
                *value["checkout_plan"]["plan"]["merge_argv_tail"]],
            "cwd": checked["cwd"], "environment_sha256": execution_environment_sha256(environment),
            "stdout_path": (
                Path(checked["stdout_path"]).parent
                / ("publication-merge-" + value["request_sha256"] + ".stdout")
            ).as_posix(),
        }
        if (native["worker_process"] != worker["worker_process"]
                or native["job_name"] != checked["job_name"]
                or native["launch_binding"] != expected_launch
                or native["read_file_custodies"] != bindings):
            _fail("PUBLICATION_GIT_LAUNCH_NATIVE_BINDING")
        instant = datetime.now(UTC)
        record = {
            "schema_version": "workflow_publication_git_launch.v1",
            "request_sha256": value["request_sha256"],
            "checkout_plan_sha256": value["checkout_plan"]["plan"]["plan_sha256"],
            "ready_sha256": canonical_digest(ready), "process": native["process"],
            "worker_process": native["worker_process"], "launch_binding": native["launch_binding"],
            "git_file_custodies": bindings[-3:], "pre_resume_sha256": canonical_digest(native),
            "observed_at": instant.isoformat(),
        }
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            current = self._head(checked["lease_id"], actor)
            if (current.execution != value
                    or self.require_publication_worker(checked, actor=actor) != worker
                    or child.pre_resume_binding() != native
                    or [item.binding() for item in files] != bindings):
                _fail("PUBLICATION_GIT_LAUNCH_CHANGED")
            self.fence._require_clean_candidate()
            value["git_launch"] = record
            _validate_publication_git_launch(value)
            self._append(current, value, instant)
        return {"status": "GIT_LAUNCH_BOUND", "binding": _copy(record),
                "dispatch_allowed": False, "resume_allowed": False, "publication_allowed": False}

    def adopt_published_attempt(
        self, lease_id: str, *, actor: str, recovery: bool = False,
    ) -> dict[str, Any]:
        """Independently attest stable C after the original entire Job has ended.

        Recovery can attest a completed physical C despite a failed/missing
        worker success receipt; it never rewrites that original failure or any
        ref. Original prepared-file identity and a fresh full-profile read remain
        mandatory. No re-execution of merge or the original Full occurs here.
        """
        from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
            observe_job,
            observe_process,
        )
        from ai_trading_system.platform.architecture.workflow_integration import (
            _require_publication_plan_absences,
        )

        if type(recovery) is not bool:
            _fail("PUBLICATION_PUBLISHED_RECOVERY_KIND")
        physical = super()._head(lease_id, actor)
        value = _copy(physical.execution)
        if value is None or not value.get("publication_attempts"):
            _fail("PUBLICATION_MISSING")
        attempt = value["publication_attempts"][-1]
        if attempt["state"] != "RESULT_RECORDED" or "git_merge" not in attempt:
            _fail("PUBLICATION_PUBLISHED_STATE")
        checked = _request(attempt["request"])
        transaction = Path(checked["publication_transaction_path"])

        def observe() -> dict[str, Any]:
            self._require_original_publication(checked, actor, merge_window=True)
            job = observe_job(checked["job_name"])["state"]
            git = observe_process(**attempt["git_launch"]["process"])
            if job not in {"EMPTY", "ABSENT"} or git["state"] not in {"EXITED", "REUSED"}:
                _fail("PUBLICATION_PUBLISHED_NOT_TERMINAL")
            _require_publication_plan_absences(attempt["checkout_plan"]["plan"]["absent_paths"])
            binding = self.fence.validate_publication_merge_window(transaction)
            scene = binding["merge_observation"]
            if not isinstance(scene, Mapping) or scene["main_sha"] != checked["candidate_sha"]:
                _fail("PUBLICATION_PUBLISHED_CANDIDATE")
            self.fence._require_clean_candidate()
            return {"job_state": job, "git_process": git, "merge_observation": scene}

        observed = observe()
        if "publication_stable_observation" in value:
            _validate_published_observation(value)
            if (value["publication_stable_observation"]["merge_observation"]
                    != observed["merge_observation"]):
                _fail("PUBLICATION_PUBLISHED_CHANGED")
            return {"status": "REPLAY_ONLY", "stable_state": "LOCAL_PUBLISHED",
                    "dispatch_allowed": False, "publication_allowed": False}
        profile = self.fence._prepare_current_full_profile(transaction, actor=actor)
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            current_observation = observe()
            if (super()._head(lease_id, actor).execution != value
                    or current_observation["merge_observation"] != observed["merge_observation"]):
                _fail("PUBLICATION_PUBLISHED_CHANGED")
            inspected = self.fence._recheck_full_profile(
                self.fence.replay(transaction), physical.execution, profile,
                phase="LOCAL_MAIN_FF_PRE",
            )
            value["publication_stable_observation"] = {
                "schema_version": "workflow_publication_stable_observation.v3",
                "stable_state": "LOCAL_PUBLISHED",
                "completion_basis": "RECOVERED_STABLE_C" if recovery else "ORIGINAL_GIT_EXIT_ZERO",
                "request_sha256": attempt["request_sha256"],
                "execution_sha256": canonical_digest(attempt), **current_observation,
                "profile_inspection": inspected, "observer": current_process_identity(),
                "observed_at": instant.isoformat(),
            }
            _validate_published_observation(value)
            super()._append(physical, value, instant)
        return {"status": "LOCAL_PUBLISHED", "candidate_sha": checked["candidate_sha"],
                "recovered": recovery, "dispatch_allowed": False, "publication_allowed": False}

    def publish_local(
        self, transaction: Path, *, actor: str, authorize_peer_head_handoff: bool = False,
    ) -> dict[str, Any]:
        """One original Full-bound publication Job; never a replacement executor."""
        from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
        from ai_trading_system.platform.architecture.workflow_execution import (
            WindowsJobProcess,
            execution_environment_sha256,
        )

        if type(authorize_peer_head_handoff) is not bool:
            _fail("PUBLICATION_PEER_HEAD_HANDOFF_APPROVAL_REQUIRED")
        transaction = self.fence._transaction_path(transaction)
        replay = self.fence.replay(transaction)
        if replay.status != "PASS" or replay.transaction["actor"] != actor:
            _fail("PUBLICATION_GIT_MERGE_TRANSACTION")
        physical = super()._head(str(replay.transaction["lease_id"]), actor)
        if physical.execution is None:
            _fail("PUBLICATION_FULL_REQUIRED")
        if physical.execution.get("publication_attempts"):
            if physical.execution.get("publication_stable_observation", {}).get(
                "stable_state"
            ) == "LOCAL_PUBLISHED":
                return self.adopt_published_attempt(physical.lease_id, actor=actor)
            return {"status": "RECOVERY_REQUIRED", "lease_id": physical.lease_id,
                    "dispatch_allowed": False, "publication_allowed": False}
        binding = self.fence.validate(transaction, exact_phase="LOCAL_MAIN_FF_PRE",
                                      require_candidate=True)
        full = full_execution_projection(physical.execution)
        intent = replay.events[-1]["payload"]["local_publication_intent"]
        if intent["topology"]["main_checkout"] is not None and not authorize_peer_head_handoff:
            _fail("PUBLICATION_PEER_HEAD_HANDOFF_APPROVAL_REQUIRED")
        identifier = canonical_digest({"transaction": replay.transaction["transaction_sha256"],
                                       "entry": "local-publish.v1", "attempt": 1})
        root = self.fence.project_root
        environment = {key: item for key, item in os.environ.items()
                       if not key.upper().startswith("GIT_")}
        environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                           GIT_OPTIONAL_LOCKS="0", PYTHONDONTWRITEBYTECODE="1",
                           PYTHONPATH=str(root / "src"))
        argv = [full["request"]["argv"][0], "-B",
                (root / "scripts/architecture_arch005_publication_fence.py").as_posix(),
                "--repository", root.as_posix(), "--policy", self.fence.policy_path.as_posix(),
                "local-publication-worker", "--transaction", transaction.absolute().as_posix(),
                "--actor", actor, "--request-id", identifier]
        if authorize_peer_head_handoff:
            argv.append("--authorize-peer-head-handoff")
        directory = Path(full["request"]["stdout_path"]).parent
        request = {key: item for key, item in full["request"].items()
                   if key != "validation_identity_sha256"}
        request.update(
            schema_version="workflow_execution_request.v5", request_id=identifier,
            execution_kind="CONTROLLED_LOCAL_PUBLICATION",
            publication_transaction_path=transaction.absolute().as_posix(),
            publication_transaction_sha256=replay.transaction["transaction_sha256"],
            local_publication_event_id=replay.events[-1]["event_id"],
            local_publication_intent_sha256=canonical_digest(intent),
            full_execution_sha256=canonical_digest(full),
            expected_main_sha=binding["expected_main_sha"],
            publication_action="PUBLISH", publication_attempt=1, previous_publication_sha256=None,
            argv=argv, environment_sha256=execution_environment_sha256(environment),
            stdout_path=(directory / ("publication-" + identifier + ".stdout")).as_posix(),
            result_path=(directory / ("publication-" + identifier + ".result.json")).as_posix(),
            job_name="Local\\AITS-DEVX015-publication-" + identifier,
        )
        request = _request(request)
        reserved = self.reserve(request, actor=actor)
        if reserved["status"] != "RESERVED":
            return {"status": "REPLAY_ONLY", "dispatch_allowed": False,
                    "publication_allowed": False}
        with WindowsJobProcess.create(
            argv=argv, cwd=root, environment=environment, stdout_path=Path(request["stdout_path"]),
            job_name=request["job_name"],
        ) as handle:
            self.bind(physical.lease_id, handle, actor=actor)
            self.resume(physical.lease_id, handle, actor=actor)
            try:
                code = handle.wait(timeout=3600)
            except TimeoutError:
                code = handle.terminate()
            self.confirm_exit(physical.lease_id, handle, actor=actor)
            result = Path(request["result_path"])
            if result.exists():
                try:
                    self.record_result(physical.lease_id, actor=actor, result_path=result)
                except (ParallelControlError, OSError, ValueError):
                    self.record_incomplete_result(physical.lease_id, actor=actor)
            else:
                self.record_incomplete_result(physical.lease_id, actor=actor)
            if code != 0:
                return {"status": "RECOVERY_REQUIRED", "lease_id": physical.lease_id,
                        "returncode": code, "dispatch_allowed": False, "publication_allowed": False}
            return self.adopt_published_attempt(physical.lease_id, actor=actor)

    def run_publication_worker(
        self, transaction: Path, *, actor: str, request_id: str,
        authorize_peer_head_handoff: bool = False,
    ) -> dict[str, Any]:
        """Fixed original worker keeps all live inputs/directories through actual merge."""
        from ai_trading_system.platform.architecture.workflow_contract import write_bound_once

        replay = self.fence.replay(transaction)
        head = self._head(str(replay.transaction["lease_id"]), actor)
        if (head.execution is None or head.execution["request"]["request_id"] != request_id
                or ("--authorize-peer-head-handoff" in head.execution["request"]["argv"])
                is not authorize_peer_head_handoff):
            _fail("PUBLICATION_WORKER_REQUEST")
        request = _request(head.execution["request"])
        self.require_publication_worker(request, actor=actor)
        root = self.fence.project_root
        result_path = Path(request["result_path"])

        def result(status: str, reason: str) -> None:
            record = {**_result_binding(request), "status": status, "reason": reason}
            write_bound_once(root, result_path.relative_to(root).as_posix(),
                             (json.dumps(record, sort_keys=True) + "\n").encode("utf-8"))

        try:
            self.record_checkout_plan(request, actor=actor)
            self.materialize_hook_capsule(request, actor=actor)
            print("PUBLICATION_STAGE hooks_created", flush=True)
            with self.prepare_hook_capsule_ready(request, actor=actor) as (_ready, files):
                print("PUBLICATION_STAGE ready_held", flush=True)
                with self.prepare_git_launch(request, files, actor=actor) as (child, held):
                    with self.switch_publication_heads(
                        request, child, held, actor=actor,
                        authorize_peer_head_handoff=authorize_peer_head_handoff,
                    ):
                        print("PUBLICATION_STAGE heads_switched", flush=True)
                        self.resume_publication_git(request, child, held, actor=actor)
                        print("PUBLICATION_STAGE merge_resumed", flush=True)
                        child.wait_exit(timeout=1800)
                        observed = self.record_publication_git_exit(request, child, actor=actor)
                        print("PUBLICATION_STAGE merge_exit", observed["returncode"], flush=True)
                        if observed["returncode"] != 0:
                            _fail("PUBLICATION_GIT_MERGE_FAILED")
            result("INSUFFICIENT", "ORIGINAL_GIT_FINISHED_REQUIRES_INDEPENDENT_ADOPTION")
            return {"status": "PUBLICATION_WORKER_PENDING_ADOPTION", "dispatch_allowed": False,
                    "publication_allowed": False}
        except BaseException as exc:
            if not result_path.exists():
                result("FAIL", str(getattr(exc, "code", type(exc).__name__)))
            raise

    def recover_local_publication(self, transaction: Path, *, actor: str) -> dict[str, Any]:
        """Resume original recovery, never dispatch a replacement publication Job."""
        transaction = self.fence._transaction_path(transaction)
        replay = self.fence.replay(transaction)
        if replay.status != "PASS" or replay.transaction["actor"] != actor:
            _fail("PUBLICATION_ORIGINAL_BINDING")
        lease_id = str(replay.transaction["lease_id"])
        terminal = self.recover(lease_id, actor=actor)
        if terminal["status"] not in {"RECOVERED_TERMINAL", "REPLAY_ONLY"}:
            return {key: item for key, item in terminal.items() if key != "execution"}
        head = self._head(lease_id, actor)
        if head.execution is None:
            _fail("PUBLICATION_MISSING")
        attempt = head.execution
        from ai_trading_system.platform.architecture.workflow_integration import _git

        main = _git(self.fence.project_root, "rev-parse", "refs/heads/main").decode().strip()
        if main == attempt["request"]["candidate_sha"]:
            return self.adopt_published_attempt(lease_id, actor=actor, recovery=True)
        if main != attempt["request"]["expected_main_sha"]:
            self._restore_failed_publication_heads(lease_id, actor=actor, main_advanced=True)
            return self.adopt_main_advanced_failed_attempt(lease_id, actor=actor)
        if "checkout_effect" in attempt:
            self._restore_failed_publication_heads(lease_id, actor=actor)
        from ai_trading_system.platform.architecture.workflow_integration import (
            inspect_local_publication_topology,
        )

        original = self._require_original_publication(attempt["request"], actor)
        topology = inspect_local_publication_topology(
            self.fence.project_root, candidate=attempt["request"]["candidate_sha"],
            expected_main=attempt["request"]["expected_main_sha"],
        )
        if (topology["candidate_checkout"]["index"]
                == original["intent"]["topology"]["candidate_checkout"]["index"]):
            return self.adopt_unchanged_failed_attempt(lease_id, actor=actor)
        return self.adopt_index_replaced_failed_attempt(lease_id, actor=actor)

    def _restore_failed_publication_heads(
        self, lease_id: str, *, actor: str, main_advanced: bool = False,
    ) -> None:
        """Original append-only intent -> same-object inverse writes -> observation.

        An interrupted recovery reuses the original intent. Only before/after
        bytes with the original native identities are recognized; torn or foreign
        state is preserved and denied. Peer private files and indexes are never
        changed. N-side recovery keeps the peer detached at M, holding N and its
        journals throughout. Neither recovery mode is successful publication.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            apply_bound_file,
            canonical_digest,
            hold_bound_directory,
        )
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
            observe_job,
            observe_process,
        )
        from ai_trading_system.platform.architecture.workflow_integration import (
            hold_publication_main_advanced_recovery_scene,
            validate_publication_main_advanced_recovery_observation,
            validate_publication_recovery_observation,
        )

        head = self._head(lease_id, actor)
        value = _copy(head.execution)
        if (value is None or value["state"] != "RESULT_RECORDED"
                or value["result"]["status"] == "PASS" or "checkout_effect" not in value):
            _fail("PUBLICATION_HEAD_RECOVERY_STATE")
        checked = value["request"]
        transaction = Path(checked["publication_transaction_path"])
        plan = value["checkout_plan"]["plan"]
        original = self._require_original_publication(
            checked, actor, recovery_window=True, main_advanced_recovery=main_advanced,
        )
        validate_scene = (validate_publication_main_advanced_recovery_observation if main_advanced
                          else validate_publication_recovery_observation)

        def observe() -> dict[str, Any]:
            if self._require_original_publication(
                checked, actor, recovery_window=True, main_advanced_recovery=main_advanced,
            ) != original:
                _fail("PUBLICATION_HEAD_RECOVERY_CHANGED")
            binding = (self.fence.validate_publication_main_advanced_recovery_window(transaction)
                       if main_advanced else self.fence.validate_publication_recovery_window(
                           transaction,
                       ))
            scene = binding["recovery_observation"]
            assert isinstance(scene, dict)
            return scene

        topology = plan["topology"]
        checkouts = [topology["candidate_checkout"]]
        if topology["main_checkout"] is not None:
            checkouts.append(topology["main_checkout"])
        with ExitStack() as stack:
            directories = [checkout[key] for checkout in checkouts
                           for key in ("root", "gitdir", "common")]
            directories += list(topology["ref_directories"].values())
            held_paths: set[str] = set()
            for directory in directories:
                path = Path(directory["path"])
                if path.as_posix() in held_paths:
                    continue
                parent = path.parent.lstat()
                stack.enter_context(hold_bound_directory(
                    path.parent, path.name, expected_identity=tuple(directory["identity"]),
                    expected_root_identity=(parent.st_dev, parent.st_ino),
                    expected_parent_identities={},
                ))
                held_paths.add(path.as_posix())
            if main_advanced:
                stack.enter_context(hold_publication_main_advanced_recovery_scene(
                    self.fence.project_root, checked, plan, value.get("git_merge"),
                    value.get("head_recovery"),
                ))
            instant = datetime.now(UTC)
            with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
                if self._head(lease_id, actor).execution != value:
                    _fail("PUBLICATION_HEAD_RECOVERY_CHANGED")
                scene = observe()
                if "head_recovery" not in value:
                    record = {
                        "schema_version": ("workflow_publication_head_recovery.v2" if main_advanced
                                           else "workflow_publication_head_recovery.v1"),
                        "request_sha256": value["request_sha256"],
                        "checkout_effect_sha256": canonical_digest(value["checkout_effect"]),
                        "git_merge_sha256": (canonical_digest(value["git_merge"])
                                             if "git_merge" in value else None),
                        "state": "INTENT", "observer": current_process_identity(),
                        "job_state": observe_job(checked["job_name"])["state"],
                        "git_process": observe_process(**value["git_launch"]["process"]),
                        "started_at": instant.isoformat(), "completed_at": None,
                        "scene_before": scene, "scene_after": None,
                        "restored_orig_head": validate_scene(
                            scene, checked, plan, value.get("git_merge"),
                        ),
                    }
                    value["head_recovery"] = record
                    _validate_publication_head_recovery(value)
                    head = self._append(head, value, instant)
                record = value["head_recovery"]
                before = record["scene_before"]
                if (scene["checkouts"]["candidate_head"]["index"]
                        != before["checkouts"]["candidate_head"]["index"]
                        or any(before["owned_locks"].get(key) != item
                               for key, item in scene["owned_locks"].items())
                        or scene["auxiliary"]["orig_head"] not in (
                            before["auxiliary"]["orig_head"], record["restored_orig_head"],
                        ) or any(scene["checkouts"][row["role"]]["head"] not in (
                            before["checkouts"][row["role"]]["head"], row["before"],
                        ) for row in plan["head_transitions"])):
                    _fail("PUBLICATION_HEAD_RECOVERY_CHANGED")
                if record["state"] == "RESTORED":
                    if scene != record["scene_after"]:
                        _fail("PUBLICATION_HEAD_RECOVERY_CHANGED")
                    return
                # Candidate releases main first. Only M recovery can restore
                # the peer's symbolic HEAD; N recovery retains detached M.
                for transition in reversed(plan["head_transitions"]):
                    role = transition["role"]
                    if main_advanced and role == "peer_head":
                        continue
                    current_head = scene["checkouts"][role]["head"]
                    if current_head == transition["before"]:
                        continue
                    checkout = (topology["candidate_checkout"] if role == "candidate_head"
                                else topology["main_checkout"])
                    apply_bound_file(
                        Path(checkout["gitdir"]["path"]), "HEAD",
                        bytes.fromhex(current_head["bytes_hex"]),
                        bytes.fromhex(transition["before"]["bytes_hex"]),
                        expected_identity=tuple(transition["before"]["identity"]),
                        expected_root_identity=tuple(checkout["gitdir"]["identity"]),
                        expected_parent_identities={},
                    )
                    scene = observe()
                current_orig = scene["auxiliary"]["orig_head"]
                if current_orig != record["restored_orig_head"]:
                    gitdir = topology["candidate_checkout"]["gitdir"]
                    restored = record["restored_orig_head"]
                    apply_bound_file(
                        Path(gitdir["path"]), "ORIG_HEAD", bytes.fromhex(current_orig["bytes_hex"]),
                        bytes.fromhex(restored["bytes_hex"]) if restored["identity"] is not None
                        else None, expected_identity=tuple(current_orig["identity"]),
                        expected_root_identity=tuple(gitdir["identity"]),
                        expected_parent_identities={},
                    )
                    scene = observe()
                for lock in list(scene["owned_locks"].values()):
                    current_checkout = topology["candidate_checkout"]
                    common = current_checkout["common"]
                    is_main = lock["path"] == (
                        Path(common["path"]) / "refs/heads/main.lock"
                    ).as_posix()
                    directory = common if is_main else current_checkout["gitdir"]
                    apply_bound_file(
                        Path(directory["path"]),
                        "refs/heads/main.lock" if is_main else "ORIG_HEAD.lock",
                        bytes.fromhex(lock["bytes_hex"]), None,
                        expected_identity=tuple(lock["identity"]),
                        expected_root_identity=tuple(directory["identity"]),
                        expected_parent_identities=(
                            {key: tuple(item["identity"]) for key, item
                             in topology["ref_directories"].items()} if is_main else {}
                        ),
                    )
                    scene = observe()
                record.update(state="RESTORED", completed_at=datetime.now(UTC).isoformat(),
                              scene_after=scene)
                _validate_publication_head_recovery(value)
                self._append(head, value, datetime.now(UTC))

    def adopt_main_advanced_failed_attempt(self, lease_id: str, *, actor: str) -> dict[str, Any]:
        """Independently adopt original failed recovery, never candidate publication."""
        from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
            observe_job,
            observe_process,
        )
        from ai_trading_system.platform.architecture.workflow_integration import (
            hold_publication_main_advanced_recovery_scene,
        )

        physical = super()._head(lease_id, actor)
        value = _copy(physical.execution)
        if value is None or not value.get("publication_attempts"):
            _fail("PUBLICATION_MISSING")
        attempt = value["publication_attempts"][-1]
        recovery = attempt.get("head_recovery")
        if (not isinstance(recovery, Mapping)
                or recovery.get("schema_version") != "workflow_publication_head_recovery.v2"
                or recovery.get("state") != "RESTORED"):
            _fail("PUBLICATION_MAIN_ADVANCED_STABLE_RECOVERY")
        checked = attempt["request"]
        transaction = Path(checked["publication_transaction_path"])
        original = self._require_original_publication(
            checked, actor, recovery_window=True, main_advanced_recovery=True,
        )
        # Prepare expensive V(C) outside the arbiter and short native custody.
        # The original recovery fence, not this profile probe, grants admission.
        profile = self.fence._prepare_current_full_profile(transaction, actor=actor)
        with hold_publication_main_advanced_recovery_scene(
            self.fence.project_root, checked, attempt["checkout_plan"]["plan"],
            attempt.get("git_merge"), recovery,
        ) as scene:
            instant = datetime.now(UTC)
            with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
                if (super()._head(lease_id, actor).execution != value
                        or self._require_original_publication(
                            checked, actor, recovery_window=True, main_advanced_recovery=True,
                        ) != original or scene != recovery["scene_after"]):
                    _fail("PUBLICATION_MAIN_ADVANCED_STABLE_CHANGED")
                self.fence._require_clean_candidate()
                native_job = observe_job(checked["job_name"])["state"]
                native_git = observe_process(**attempt["git_launch"]["process"])
                if native_job not in {"EMPTY", "ABSENT"} or native_git["state"] not in {
                    "EXITED", "REUSED",
                }:
                    _fail("PUBLICATION_MAIN_ADVANCED_STABLE_PROCESS")
                inspected = self.fence._recheck_full_profile(
                    self.fence.replay(transaction), physical.execution, profile,
                    phase="LOCAL_MAIN_FF_PRE",
                )
                current_scene = self.fence.validate_publication_main_advanced_recovery_window(
                    transaction,
                )["recovery_observation"]
                if current_scene != scene:
                    _fail("PUBLICATION_MAIN_ADVANCED_STABLE_CHANGED")
                if "publication_stable_observation" in value:
                    _validate_main_advanced_failed_observation(value)
                    return {"status": "REPLAY_ONLY", "dispatch_allowed": False,
                            "publication_allowed": False,
                            "stable_state": "CANDIDATE_RETAINED_MAIN_ADVANCED"}
                value["publication_stable_observation"] = {
                    "schema_version": "workflow_publication_stable_observation.v4",
                    "stable_state": "CANDIDATE_RETAINED_MAIN_ADVANCED",
                    "request_sha256": attempt["request_sha256"],
                    "execution_sha256": canonical_digest(attempt),
                    "head_recovery_sha256": canonical_digest(recovery),
                    "recovery_observation": scene, "profile_inspection": inspected,
                    "git_process": native_git, "job_state": native_job,
                    "observer": current_process_identity(), "observed_at": instant.isoformat(),
                    "dispatch_allowed": False, "publication_allowed": False,
                }
                _validate_main_advanced_failed_observation(value)
                super()._append(physical, value, instant)
        return {"status": "RECOVERED_FAILED", "dispatch_allowed": False,
                "publication_allowed": False,
                "stable_state": "CANDIDATE_RETAINED_MAIN_ADVANCED",
                "candidate_sha": checked["candidate_sha"], "main_sha": scene["main_sha"]}

    def adopt_unchanged_failed_attempt(self, lease_id: str, *, actor: str) -> dict[str, Any]:
        """Independently close a failed attempt that left original checkout intact.

        No worker result can invoke this as a serialized capability. A changed
        ref/index/checkout still needs the distinct contained recovery path.
        """
        return self._adopt_failed_attempt(lease_id, actor=actor, index_replaced=False)

    def adopt_index_replaced_failed_attempt(self, lease_id: str, *, actor: str) -> dict[str, Any]:
        """Close a failed attempt with a verified C-equivalent replacement index.

        No candidate/index/ref writes and no publication PASS or redispatch. All
        other original topology and current V(C) remain mandatory authority.
        """
        return self._adopt_failed_attempt(lease_id, actor=actor, index_replaced=True)

    def _adopt_failed_attempt(
        self, lease_id: str, *, actor: str, index_replaced: bool,
    ) -> dict[str, Any]:
        from ai_trading_system.platform.architecture.parallel_control_kernel import (
            _canonical_sha256,
        )
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
            observe_job,
        )

        physical = super()._head(lease_id, actor)
        value = _copy(physical.execution)
        if value is None or not value.get("publication_attempts"):
            _fail("PUBLICATION_MISSING")
        attempt = value["publication_attempts"][-1]
        if attempt["state"] != "RESULT_RECORDED" or attempt["result"]["status"] == "PASS":
            _fail("PUBLICATION_STABLE_STATE")
        checked = attempt["request"]
        original = self._require_original_publication(checked, actor)
        transaction = Path(checked["publication_transaction_path"])
        if observe_job(checked["job_name"])["state"] not in {"EMPTY", "ABSENT"}:
            _fail("PUBLICATION_JOB_STILL_RUNNING")
        preparation_resolution = _observe_unchanged_preparation(attempt)
        git_launch_resolution = _observe_unchanged_git_launch(attempt)
        profile = None
        if index_replaced and "publication_stable_observation" not in value:
            profile = self.fence._prepare_current_full_profile(transaction, actor=actor)

        def inspect() -> dict[str, Any]:
            if not index_replaced:
                return self.fence.inspect_local_publication(transaction)
            from ai_trading_system.platform.architecture.workflow_integration import (
                inspect_local_publication_topology,
            )

            self.fence._require_clean_candidate()
            topology = inspect_local_publication_topology(
                self.fence.project_root, candidate=checked["candidate_sha"],
                expected_main=checked["expected_main_sha"],
            )
            _require_index_replaced_topology(original["intent"]["topology"], topology)
            checkout = topology["candidate_checkout"]
            for path in (Path(checkout["common"]["path"]) / "refs/heads/main.lock",
                         Path(checkout["gitdir"]["path"]) / "HEAD.lock",
                         Path(checkout["gitdir"]["path"]) / "index.lock"):
                try:
                    path.lstat()
                except FileNotFoundError:
                    continue
                _fail("PUBLICATION_INDEX_RECOVERY_LOCK_REMAINS")
            return {"topology": topology}

        observed = inspect()
        stable_state = ("CANDIDATE_STABLE_INDEX_REPLACED" if index_replaced
                        else "ORIGINAL_UNCHANGED")
        instant = datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
            if (super()._head(lease_id, actor).execution != value
                    or self._require_original_publication(checked, actor) != original
                    or inspect() != observed):
                _fail("PUBLICATION_STABLE_CHANGED")
            if observe_job(checked["job_name"])["state"] not in {"EMPTY", "ABSENT"}:
                _fail("PUBLICATION_JOB_STILL_RUNNING")
            current_preparation = _observe_unchanged_preparation(attempt)
            if not _same_terminal_process_resolution(preparation_resolution, current_preparation):
                _fail("PUBLICATION_PREPARATION_RESOLUTION_CHANGED")
            current_launch = _observe_unchanged_git_launch(attempt)
            if not _same_terminal_process_resolution(git_launch_resolution, current_launch):
                _fail("PUBLICATION_GIT_LAUNCH_RESOLUTION_CHANGED")
            preparation_resolution = current_preparation
            git_launch_resolution = current_launch
            if "publication_stable_observation" in value:
                _validate_unchanged_publication_observation(value)
                stable = value["publication_stable_observation"]
                if (stable["stable_state"] != stable_state
                        or stable["topology"] != observed["topology"]):
                    _fail("PUBLICATION_STABLE_CHANGED")
                return {"status": "REPLAY_ONLY", "dispatch_allowed": False,
                        "publication_allowed": False, "stable_state": stable_state}
            value["publication_stable_observation"] = {
                "schema_version": ("workflow_publication_stable_observation.v2" if index_replaced
                                   else "workflow_publication_stable_observation.v1"),
                "stable_state": stable_state, "request_sha256": attempt["request_sha256"],
                "execution_sha256": _canonical_sha256(attempt), "topology": observed["topology"],
                "observer": current_process_identity(), "observed_at": instant.isoformat(),
            }
            if index_replaced:
                inspected = self.fence._recheck_full_profile(
                    self.fence.replay(transaction), physical.execution, profile,
                    phase="LOCAL_MAIN_FF_PRE",
                )
                value["publication_stable_observation"].update(
                    original_topology=_copy(original["intent"]["topology"]),
                    profile_inspection=inspected,
                )
            if preparation_resolution is not None:
                value["publication_stable_observation"]["preparation_resolution"] = (
                    preparation_resolution
                )
            if git_launch_resolution is not None:
                value["publication_stable_observation"]["git_launch_resolution"] = (
                    git_launch_resolution
                )
            super()._append(physical, value, instant)
        return {"status": "STABLE_FAILED_ATTEMPT", "dispatch_allowed": False,
                "publication_allowed": False, "stable_state": stable_state}

    @contextmanager
    def prepare_main_reference(
        self, request: Mapping[str, Any], *, actor: str,
    ) -> Iterator[dict[str, Any]]:
        """Hold actual Git preparation and persist its custody in this attempt.

        This preparatory component intentionally aborts on exit. It does not yet
        expose commit: the contained checkout transition and independent stable
        adopter must be connected before a public publisher can consume it.
        Neither an ACK nor this record is publication PASS or recovery authority.
        """
        from ai_trading_system.platform.architecture.workflow_contract import hold_bound_directory

        checked = _request(request)
        self.require_publication_worker(checked, actor=actor)
        original = self._require_original_publication(checked, actor)
        head = self._head(checked["lease_id"], actor)
        if (head.execution is None or head.execution["state"] != "RUNNING"
                or "main_preparation" in head.execution):
            _fail("PUBLICATION_PREPARATION_STATE")
        transaction = Path(checked["publication_transaction_path"])
        profile = self.fence._prepare_current_full_profile(transaction, actor=actor)
        topology = self.fence.inspect_local_publication(
            Path(checked["publication_transaction_path"])
        )["topology"]
        directories = topology["ref_directories"]
        with hold_bound_directory(
            Path(topology["candidate_checkout"]["common"]["path"]), "refs/heads",
            expected_identity=tuple(directories["refs/heads"]["identity"]),
            expected_root_identity=tuple(topology["candidate_checkout"]["common"]["identity"]),
            expected_parent_identities={"refs": tuple(directories["refs"]["identity"])},
        ) as custody:
            if self.fence.inspect_local_publication(
                Path(checked["publication_transaction_path"])
            )["topology"] != topology:
                _fail("PUBLICATION_PREPARATION_CHANGED")
            with self.store.atomic(actor=actor, now=datetime.now(UTC),
                                   operation="execution_complete"):
                if (self._head(checked["lease_id"], actor).execution != head.execution
                        or self._require_original_publication(checked, actor) != original):
                    _fail("PUBLICATION_PREPARATION_CHANGED")
                self.fence._require_clean_candidate()
                self.fence._recheck_full_profile(
                    self.fence.replay(transaction),
                    super()._head(checked["lease_id"], actor).execution,
                    profile, phase="LOCAL_MAIN_FF_PRE",
                )
            with self._prepare_main_reference_held(
                checked, actor=actor, head=head, original=original, topology=topology,
                custody=custody, profile=profile,
            ) as record:
                yield record

    @contextmanager
    def _prepare_main_reference_held(
        self, checked: Mapping[str, Any], *, actor: str, head: ExecutionLease,
        original: Mapping[str, Any], topology: Mapping[str, Any], custody: _BoundDirectoryCustody,
        profile: _RemotePreparation,
    ) -> Iterator[dict[str, Any]]:
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
        from ai_trading_system.platform.architecture.workflow_execution import (
            contained_subprocess_identity,
            current_job_member,
            execution_environment_sha256,
        )

        if head.execution is None:
            _fail("PUBLICATION_PREPARATION_STATE")
        common = Path(topology["candidate_checkout"]["common"]["path"])
        lock = common / "refs/heads/main.lock"
        if lock.exists() or lock.is_symlink():
            _fail("PUBLICATION_PREPARATION_LOCK_EXISTS")
        executable = shutil.which("git")
        if executable is None:
            _fail("PUBLICATION_GIT_UNAVAILABLE")
        executable = Path(executable).resolve().as_posix()
        executable_metadata = Path(executable).stat()
        executable_identity = (executable_metadata.st_dev, executable_metadata.st_ino)
        executable_sha = hashlib.sha256(bounded_regular_bytes(
            Path(executable), expected_identity=executable_identity,
            expected_link_count=executable_metadata.st_nlink,
        )).hexdigest()
        environment = {key: value for key, value in os.environ.items()
                       if not key.upper().startswith("GIT_")}
        environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                           GIT_OPTIONAL_LOCKS="0")
        worker = current_job_member(checked["job_name"])
        argv = [executable, "update-ref", "--stdin"]
        # The original worker is already in its Job. This fixed child inherits
        # that containment at creation; verify its original native handle before
        # sending any ref transaction bytes. No breakaway flag is used.
        with custody.subprocess_inheritance() as startup:
            process = subprocess.Popen(
                argv, cwd=checked["cwd"], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment,
                startupinfo=startup, close_fds=True, creationflags=subprocess.CREATE_NO_WINDOW,
            )
        reader = ThreadPoolExecutor(max_workers=1)
        try:
            git_identity = contained_subprocess_identity(process, checked["job_name"])
            assert process.stdin is not None and process.stdout is not None
            # Text-mode pipes translate LF to CRLF on Windows; the Git protocol
            # requires exact LF bytes, as characterized by v109/v110.
            commands = (
                "start\nupdate refs/heads/main " + checked["candidate_sha"] + " "
                + checked["expected_main_sha"] + "\nprepare\n"
            ).encode("ascii")
            process.stdin.write(commands)
            process.stdin.flush()
            acknowledgements = [
                reader.submit(process.stdout.readline).result(timeout=15) for _ in range(2)
            ]
            if acknowledgements != [b"start: ok\n", b"prepare: ok\n"]:
                _fail("PUBLICATION_PREPARATION_ACK")
            metadata = lock.lstat()
            identity = (metadata.st_dev, metadata.st_ino)
            wanted = (checked["candidate_sha"] + "\n").encode("ascii")
            if bounded_regular_bytes(lock, expected_identity=identity) != wanted:
                _fail("PUBLICATION_PREPARATION_CONTENT")
            instant = datetime.now(UTC)
            record = {
                "schema_version": "workflow_publication_main_preparation.v1",
                "request_sha256": head.execution["request_sha256"], "topology": topology,
                "worker_process": worker, "git_process": git_identity, "argv": argv,
                "git_executable": {"path": executable, "identity": list(executable_identity),
                                   "link_count": executable_metadata.st_nlink,
                                   "sha256": executable_sha},
                "git_environment_sha256": execution_environment_sha256(environment),
                "prepared_ref": {"path": lock.as_posix(), "identity": list(identity),
                                 "sha256": hashlib.sha256(wanted).hexdigest()},
                "acknowledgements": [item.decode("ascii") for item in acknowledgements],
                "observed_at": instant.isoformat(),
            }
            with self.store.atomic(actor=actor, now=instant, operation="execution_complete"):
                current = self._head(checked["lease_id"], actor)
                if (current.execution != head.execution
                        or self._require_original_publication(checked, actor) != original
                        or self.fence.inspect_local_publication(
                            Path(checked["publication_transaction_path"])
                        )["topology"] != topology
                        or contained_subprocess_identity(process, checked["job_name"])
                        != git_identity
                        or current_job_member(checked["job_name"]) != worker
                        or bounded_regular_bytes(lock, expected_identity=identity) != wanted):
                    _fail("PUBLICATION_PREPARATION_CHANGED")
                self.fence._require_clean_candidate()
                self.fence._recheck_full_profile(
                    self.fence.replay(Path(checked["publication_transaction_path"])),
                    super()._head(checked["lease_id"], actor).execution,
                    profile, phase="LOCAL_MAIN_FF_PRE",
                )
                value = _copy(current.execution)
                value["main_preparation"] = record
                self._append(current, value, instant)
            yield _copy(record)
        finally:
            try:
                if process.poll() is None:
                    stdout, stderr = process.communicate(b"abort\n", timeout=20)
                    if process.returncode != 0 or stdout != b"abort: ok\n" or stderr:
                        _fail("PUBLICATION_PREPARATION_ABORT")
            finally:
                # Own this exact still-live child only; never delete a remaining
                # lock by pathname or substitute a different process/store.
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=20)
                reader.shutdown(wait=True, cancel_futures=True)
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()

    def reserve(
        self, request: Mapping[str, Any], *, actor: str, now: datetime | None = None,
    ) -> dict[str, Any]:
        from ai_trading_system.platform.architecture.parallel_control_kernel import (
            _canonical_sha256,
        )
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
        )

        checked = _request(request)
        original = self._require_original_publication(checked, actor)
        observed = self.fence.inspect_local_publication(
            Path(checked["publication_transaction_path"]),
        )
        if observed["head_event_id"] != original["event_id"]:
            _fail("PUBLICATION_ORIGINAL_CHANGED")
        instant = now or datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_reserve"):
            if self._require_original_publication(checked, actor) != original:
                _fail("PUBLICATION_ORIGINAL_CHANGED")
            # Reobserve the exact original checkout immediately before the
            # event append; metadata observations alone never authorize effects.
            if self.fence.inspect_local_publication(
                Path(checked["publication_transaction_path"])
            ) != observed:
                _fail("PUBLICATION_ORIGINAL_CHANGED")
            physical = super()._head(checked["lease_id"], actor)
            outer = _copy(physical.execution)
            attempts = outer.get("publication_attempts", [])
            for lease in self.store.replay().lease_heads:
                if lease.execution is None:
                    continue
                for execution in [
                    lease.execution, *lease.execution.get("installation_attempts", []),
                    *lease.execution.get("publication_attempts", []),
                ]:
                    if execution["request"]["request_id"] == checked["request_id"]:
                        if lease.lease_id != physical.lease_id or execution["request"] != checked:
                            _fail("REQUEST_REUSE_MISMATCH")
                        return {"status": "REPLAY_ONLY", "dispatch_allowed": False,
                                "execution": _copy(execution)}
            value = {
                "schema_version": "lease_execution.v1", "request": checked,
                "request_sha256": _canonical_sha256(checked),
                "launcher": current_process_identity(),
                "state": "RESERVED", "process": None, "exit": None, "result": None,
            }
            outer["schema_version"] = "lease_execution.v5"
            outer["publication_attempts"] = [*attempts, value]
            updated = super()._append(physical, outer, instant)
            if updated.execution is None:
                _fail("PUBLICATION_MISSING")
            return {"status": "RESERVED", "dispatch_allowed": True,
                    "execution": _copy(updated.execution["publication_attempts"][-1])}


class InstallationLifecycle(ExecutionLifecycle):
    """Contained install/recovery attempts under the original source lease.

    Original source custody is immutable. Each failed contained attempt remains
    in the same event chain; only a separately verified stable result permits
    lease release. The short arbiter never encloses installation or Job waits.
    """

    def _head(self, lease_id: str, actor: str, *, require_active: bool = True) -> ExecutionLease:
        physical = super()._head(lease_id, actor, require_active=require_active)
        if physical.execution is None or not physical.execution.get("installation_attempts"):
            _fail("INSTALLATION_MISSING")
        return replace(physical, execution=_copy(physical.execution["installation_attempts"][-1]))

    def _append(
        self, head: ExecutionLease, value: Mapping[str, Any], now: datetime
    ) -> ExecutionLease:
        physical = super()._head(head.lease_id, head.actor)
        outer = _copy(physical.execution)
        if not outer or outer.get("installation_attempts", [None])[-1] != head.execution:
            _fail("INSTALLATION_STATE_CHANGED")
        outer["installation_attempts"][-1] = _copy(value)
        updated = super()._append(physical, outer, now)
        return replace(updated, execution=_copy(value))

    def require_installation_worker(
        self, request: Mapping[str, Any], *, actor: str
    ) -> dict[str, Any]:
        return self._require_worker(request, actor=actor, source_candidate=True, installation=True)

    def record_created_object(
        self, request: Mapping[str, Any], descriptor: int, *, actor: str
    ) -> dict[str, Any]:
        """Record the actual held file, never a caller-supplied identity claim.

        Called while native creation still has delete-on-close armed. The
        caller must not clear it until this original event append succeeds.
        """
        import ctypes
        import msvcrt
        import stat
        from ctypes import wintypes as w

        from ai_trading_system.platform.architecture.workflow_integration import (
            _installation_context,
            _installation_directory_rows,
        )

        self.require_installation_worker(request, actor=actor)
        physical = super()._head(request["lease_id"], actor)
        if physical.execution is None:
            _fail("INSTALLATION_MISSING")
        plan, targets = _installation_context(
            Path(request["cwd"]), request, physical.execution, actor
        )
        if type(descriptor) is not int or descriptor < 0:
            _fail("CREATION_DESCRIPTOR")
        api = ctypes.WinDLL("kernel32", use_last_error=True)
        api.GetFinalPathNameByHandleW.argtypes = [w.HANDLE, w.LPWSTR, w.DWORD, w.DWORD]
        api.GetFinalPathNameByHandleW.restype = w.DWORD

        def observe() -> tuple[Path, tuple[int, int], bool]:
            info = os.fstat(descriptor)
            name = ctypes.create_unicode_buffer(32768)
            length = api.GetFinalPathNameByHandleW(
                msvcrt.get_osfhandle(descriptor), name, len(name), 0
            )
            if (
                not length
                or length >= len(name)
                or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))
                or (stat.S_ISREG(info.st_mode) and (info.st_nlink != 1 or info.st_size != 0))
            ):
                _fail("CREATION_HANDLE")
            path = name.value
            if path.startswith("\\\\?\\UNC\\"):
                path = "\\\\" + path[8:]
            elif path.startswith("\\\\?\\"):
                path = path[4:]
            return Path(path), (info.st_dev, info.st_ino), stat.S_ISDIR(info.st_mode)

        path, identity, is_directory = observe()
        if is_directory:
            targets = [
                (
                    Path(plan[row["root_key"]]["path"]),
                    (plan[row["root_key"]]["device"], plan[row["root_key"]]["file_id"]),
                    row,
                )
                for row in _installation_directory_rows(plan)
            ]
        matches = [
            (base, root_id, row)
            for base, root_id, row in targets
            if os.path.normcase(str(base / row["path"])) == os.path.normcase(str(path))
        ]
        if len(matches) != 1:
            _fail("CREATION_TARGET")
        base, root_identity, row = matches[0]
        wanted = None
        if is_directory:
            if row["identity"] is not None:
                _fail("CREATION_TARGET")
        else:
            wanted = row[
                "after_hex" if request["installation_action"] == "INSTALL" else "before_hex"
            ]
            if wanted is None or (
                request["installation_action"] == "INSTALL" and row["before_hex"] is not None
            ):
                _fail("CREATION_TARGET")
        with self.store.atomic(actor=actor, now=datetime.now(UTC), operation="execution_complete"):
            witness = self.require_installation_worker(request, actor=actor)
            head = self._head(request["lease_id"], actor)
            value = _copy(head.execution)
            if value["schema_version"] != "lease_execution.v3" or value["state"] != "RUNNING":
                _fail("CREATION_STATE")
            if observe() != (path, identity, is_directory) or directory_identity(base) != {
                "path": base.as_posix(),
                "device": root_identity[0],
                "file_id": root_identity[1],
            }:
                _fail("CREATION_HANDLE_CHANGED")
            record = {
                "plan_sha256": request["installation_plan_sha256"],
                "root": base.as_posix(),
                "root_identity": list(root_identity),
                "path": row["path"],
                "file_identity": list(identity),
                "worker_process": witness["worker_process"],
            }
            if is_directory:
                record["kind"] = "directory"
            else:
                if not isinstance(wanted, str):
                    _fail("CREATION_TARGET")
                record["target_sha256"] = hashlib.sha256(bytes.fromhex(wanted)).hexdigest()
            for previous in value["created_objects"]:
                if (previous["root"].casefold(), previous["path"].casefold()) == (
                    record["root"].casefold(),
                    record["path"].casefold(),
                ):
                    if previous != record:
                        _fail("CREATION_REUSE_MISMATCH")
                    return dict(_copy(previous))
            value["created_objects"].append(record)
            updated = self._append(head, value, datetime.now(UTC))
            if updated.execution is None:
                _fail("INSTALLATION_MISSING")
            return dict(_copy(updated.execution["created_objects"][-1]))

    def adopt_stable_result(self, lease_id: str, *, actor: str) -> dict[str, Any]:
        """Accept only independently observed original-plan checkout stability.

        The generic record_result API remains unable to create installation
        PASS. Expensive file/Git verification stays outside the short arbiter.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            apply_bound_file,
            bounded_regular_bytes,
        )
        from ai_trading_system.platform.architecture.workflow_integration import (
            _installation_context,
            _installation_directories,
            _installation_lock_bytes,
            _installation_locks,
            _installation_parent_identities,
            _verify_source_installation,
        )
        from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

        with self.store.atomic(actor=actor, now=datetime.now(UTC), operation="execution_complete"):
            physical = super()._head(lease_id, actor)
            head = self._head(lease_id, actor)
            value = _copy(head.execution)
            if (
                value is None
                or value["state"] != "EXIT_CONFIRMED"
                or value["exit"]["returncode"] != 0
            ):
                _fail("INSTALLATION_ADOPTION_STATE")
        request = value["request"]
        root = Path(request["cwd"])
        path = Path(request["result_path"])
        if physical.execution is None:
            _fail("INSTALLATION_MISSING")
        content = bounded_regular_bytes(path)
        result = load_strict_json_text(content.decode())
        if not isinstance(result, dict):
            _fail("INSTALLATION_ADOPTION_RESULT")
        if result.get("status") != "PASS" or any(
            result.get(key) != expected for key, expected in _result_binding(request).items()
        ):
            _fail("INSTALLATION_ADOPTION_RESULT")
        plan, _targets = _installation_context(root, request, physical.execution, actor)
        locks = _installation_locks(plan)
        lock_bytes = _installation_lock_bytes(request)
        for base, _identity, name in locks:
            if bounded_regular_bytes(base / name) != lock_bytes:
                _fail("INSTALLATION_LOCK_CHANGED")
        observed = _verify_source_installation(
            root, request, physical.execution, actor, allow_recovery_directories=True
        )
        if result.get("stable_state") != observed["stable_state"]:
            _fail("INSTALLATION_ADOPTION_RESULT")
        # Remove only exact plan-owned Git interoperability locks after actual
        # stable verification. The original workflow lease remains protected.
        for base, identity, name in reversed(locks):
            info = (base / name).lstat()
            apply_bound_file(
                base,
                name,
                lock_bytes,
                None,
                expected_identity=(info.st_dev, info.st_ino),
                expected_root_identity=identity,
                expected_parent_identities=_installation_parent_identities(
                    plan, physical.execution, base, name
                ),
            )
        if request["installation_action"] == "RECOVER":
            _installation_directories(plan, physical.execution, restored=True, remove=True)
        # Recheck after unlocking; a new head/main cannot inherit this adoption.
        observed = _verify_source_installation(root, request, physical.execution, actor)
        with self.store.atomic(actor=actor, now=datetime.now(UTC), operation="execution_complete"):
            if (
                self._head(lease_id, actor).execution != value
                or bounded_regular_bytes(path) != content
            ):
                _fail("INSTALLATION_ADOPTION_CHANGED")
            value["state"] = "RESULT_RECORDED"
            value["result"] = {
                "status": "PASS",
                "reason": "INDEPENDENT_SOURCE_INSTALLATION_VERIFIED",
                "artifact": {
                    "path": path.as_posix(),
                    "sha256": hashlib.sha256(content).hexdigest(),
                },
            }
            self._append(head, value, datetime.now(UTC))
        return observed

    def reserve(
        self, request: Mapping[str, Any], *, actor: str, now: datetime | None = None
    ) -> dict[str, Any]:
        from ai_trading_system.platform.architecture.parallel_control_kernel import (
            _canonical_sha256,
            _lease_expiry,
        )
        from ai_trading_system.platform.architecture.workflow_execution import (
            current_process_identity,
        )

        checked = _request(request)
        if checked["schema_version"] != "workflow_execution_request.v4":
            _fail("INSTALLATION_REQUEST_REQUIRED")
        instant = now or datetime.now(UTC)
        with self.store.atomic(actor=actor, now=instant, operation="execution_reserve"):
            physical = super()._head(checked["lease_id"], actor, require_active=False)
            outer = _copy(physical.execution)
            if (
                outer is None
                or outer["request"]["schema_version"] != "workflow_execution_request.v3"
                or outer["state"] != "RESULT_RECORDED"
                or outer["result"]["status"] != "PASS"
            ):
                _fail("INSTALLATION_SOURCE_REQUIRED")
            attempts = outer.get("installation_attempts", [])
            for lease in self.store.replay().lease_heads:
                if lease.execution is None:
                    continue
                for execution in [
                    lease.execution,
                    *lease.execution.get("installation_attempts", []),
                ]:
                    if execution["request"]["request_id"] == checked["request_id"]:
                        if lease.lease_id != physical.lease_id or execution["request"] != checked:
                            _fail("REQUEST_REUSE_MISMATCH")
                        return {
                            "status": "REPLAY_ONLY",
                            "dispatch_allowed": False,
                            "execution": _copy(execution),
                        }
            if physical.state != "ACTIVE":
                _fail("ACTIVE_OWNER")
            if not attempts and _lease_expiry(physical) <= instant:
                _fail("LEASE_EXPIRED")
            # Once installation has started, TTL cannot strand a stable-state
            # recovery. validate_execution below requires a terminal failed
            # predecessor and an exact linked recovery request, never redispatch.
            value: dict[str, Any] = {
                "schema_version": "lease_execution.v3",
                "request": checked,
                "request_sha256": _canonical_sha256(checked),
                "launcher": current_process_identity(),
                "state": "RESERVED",
                "process": None,
                "exit": None,
                "result": None,
                "created_objects": [],
            }
            outer["schema_version"] = "lease_execution.v2"
            outer["installation_attempts"] = [*attempts, value]
            updated = super()._append(physical, outer, instant)
            if updated.execution is None:
                _fail("INSTALLATION_MISSING")
            return {
                "status": "RESERVED",
                "dispatch_allowed": True,
                "execution": _copy(updated.execution["installation_attempts"][-1]),
            }


CONTROL_BINDING_NAME = "aits-workflow-control.v1.json"
CONTROL_STATE_NAME = "host-control.v1.json"
RETIREMENT_NAME = "workflow-retirement.v1.json"
# DEVX-015A: the anchor is one REG_SZ value on the protected parent key. A single
# value write is atomic (absent = not enrolled), so no staged key or RegRenameKey
# is needed; RegRenameKey crashed this host's kernel twice (NtRenameKey, 0xBE/0x3B).
HOST_REGISTRY_KEY = r"SOFTWARE\AITradingSystem"
HOST_REGISTRY_VALUE = "WorkflowControl.RegistrationV1"
# Pre-DEVX-015A subkey anchor. Never written on any enrolled host; if present its
# origin is unknown, so every reader and writer fails closed instead of adopting it.
LEGACY_HOST_REGISTRY_KEY = r"SOFTWARE\AITradingSystem\WorkflowControl"
_DRAIN_OPERATIONS = {"observe", "heartbeat", "terminal", "execution_complete"}


def _control_fail(code: str) -> NoReturn:
    raise ParallelControlError("WORKFLOW_CONTROL_" + code, "共享协调写入门禁拒绝")


def machine_host_id() -> str:
    """OS machine identity, not a caller-supplied environment label."""
    if os.name != "nt":
        _control_fail("PLATFORM_UNSUPPORTED")
    import winreg

    with winreg.OpenKey(
        winreg.HKEY_LOCAL_MACHINE,
        r"SOFTWARE\Microsoft\Cryptography",
        0,
        winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
    ) as key:
        machine, _kind = winreg.QueryValueEx(key, "MachineGuid")
    return hashlib.sha256(str(machine).casefold().encode()).hexdigest()


def directory_identity(path: Path) -> dict[str, Any]:
    import stat

    path = path.absolute()
    if path != path.resolve(strict=True):
        _control_fail("ROOT_ALIAS")
    for entry in (*path.parents, path):
        value = entry.lstat()
        if stat.S_ISLNK(value.st_mode) or getattr(value, "st_file_attributes", 0) & 0x400:
            _control_fail("ROOT_REPARSE")
    value = path.stat()
    if not stat.S_ISDIR(value.st_mode):
        _control_fail("ROOT_NOT_DIRECTORY")
    return {"path": path.as_posix(), "device": value.st_dev, "file_id": value.st_ino}


def _control_json(path: Path, *, expected_sha256: str | None = None) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    content = bounded_regular_bytes(path, budget=1024 * 1024)
    if expected_sha256 is not None and hashlib.sha256(content).hexdigest() != expected_sha256:
        _control_fail("HOST_REGISTRATION_CHANGED")
    value = load_strict_json_text(content.decode("utf-8"))
    if not isinstance(value, dict):
        _control_fail("STATE_OBJECT")
    return value


def _trusted_host_registration() -> dict[str, Any] | None:
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    raw = _trusted_host_registration_bytes()
    if raw is None:
        return None
    return _validate_host_registration(load_strict_json_text(raw.decode("utf-8")))


def _reject_legacy_host_registry_key() -> None:
    """Read-only probe; the legacy subkey is never opened for write or adopted."""
    import winreg

    try:
        legacy = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            LEGACY_HOST_REGISTRY_KEY,
            0,
            winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
        )
    except FileNotFoundError:
        return
    with legacy:
        _control_fail("HOST_REGISTRATION_LEGACY_KEY")


def _trusted_host_registration_bytes() -> bytes | None:
    """Machine-admin anchor, outside Git and caller-selected lease directories.

    Installation/updates must protect this HKLM key and the control-state files
    from participant writes. An unreadable or damaged existing registration is
    not a pre-installation host. No environment or per-checkout override exists.
    The anchor is one value on the parent key: a missing key or value means not
    enrolled; a legacy WorkflowControl subkey is of unknown origin and fails closed.
    """
    if os.name != "nt":
        _control_fail("PLATFORM_UNSUPPORTED")
    import winreg

    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    _reject_legacy_host_registry_key()
    try:
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            HOST_REGISTRY_KEY,
            0,
            winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
        )
    except FileNotFoundError:
        return None
    with key:
        try:
            content, kind = winreg.QueryValueEx(key, HOST_REGISTRY_VALUE)
        except FileNotFoundError:
            return None
    if kind != winreg.REG_SZ or not isinstance(content, str) or len(content) > 1024 * 1024:
        _control_fail("HOST_REGISTRATION_VALUE")
    _validate_host_registration(load_strict_json_text(content))
    # Preserve REG_SZ text exactly as UTF-8 journal bytes. Re-serializing the
    # parsed dictionary would erase formatting differences during recovery.
    return content.encode("utf-8")


def _validate_host_registration(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "host_id",
            "control_root",
            "root_identity",
            "state_sha256",
            "repositories",
        }
        or value["schema_version"] != "workflow_machine_registration.v1"
        or value["host_id"] != machine_host_id()
        or not isinstance(value["control_root"], str)
        or not Path(value["control_root"]).is_absolute()
        or value["root_identity"] != directory_identity(Path(value["control_root"]))
        or not isinstance(value["repositories"], list)
        or not value["repositories"]
    ):
        _control_fail("HOST_REGISTRATION_SCHEMA")
    _control_digest(value["state_sha256"])
    common_ids: set[str] = set()
    checkout_ids: set[str] = set()
    for row in value["repositories"]:
        if not isinstance(row, dict) or set(row) != {
            "common_identity",
            "checkout_identities",
            "locator_sha256",
        }:
            _control_fail("HOST_REPOSITORY_SCHEMA")
        _validate_directory_identity(row["common_identity"])
        _control_digest(row["locator_sha256"])
        common = row["common_identity"]["path"].casefold()
        if common in common_ids:
            _control_fail("HOST_REPOSITORY_DUPLICATE")
        common_ids.add(common)
        if not isinstance(row["checkout_identities"], list) or not row["checkout_identities"]:
            _control_fail("HOST_CHECKOUT_SCHEMA")
        for checkout in row["checkout_identities"]:
            _validate_directory_identity(checkout)
            key = checkout["path"].casefold()
            if key in checkout_ids:
                _control_fail("HOST_CHECKOUT_DUPLICATE")
            checkout_ids.add(key)
    return value


def _control_digest(value: Any) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        _control_fail("DIGEST_SCHEMA")


def _trusted_repository(
    registration: dict[str, Any],
    *,
    project: Mapping[str, Any],
    common: Mapping[str, Any],
) -> dict[str, Any]:
    matches = [row for row in registration["repositories"] if row["common_identity"] == common]
    if len(matches) != 1 or project not in matches[0]["checkout_identities"]:
        _control_fail("HOST_REPOSITORY_NOT_REGISTERED")
    return dict(matches[0])


def _cutover_publication_position(
    *,
    before_state: bytes,
    after_state: bytes,
    before_registration: bytes,
    after_registration: bytes,
    observed_state: bytes,
    observed_registration: bytes,
) -> str:
    """Classify exact journal bytes, never grant custody or activation authority.

    The caller must independently authenticate the protected journal and hold
    cutover custody. This pure check deliberately cannot write or repair state.
    State publication precedes registration publication; reverse order is not a
    recovery position, even when each individual document looks legitimate.
    """
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    def document(raw: bytes) -> dict[str, Any]:
        if not isinstance(raw, bytes) or not raw or len(raw) > 1024 * 1024:
            _control_fail("CUTOVER_TRANSITION_BYTES")
        try:
            value = load_strict_json_text(raw.decode("utf-8"))
        except (ValueError, UnicodeError):
            _control_fail("CUTOVER_TRANSITION_JSON")
        if not isinstance(value, dict):
            _control_fail("CUTOVER_TRANSITION_JSON")
        return value

    old, new = document(before_state), document(after_state)
    old_registration, new_registration = (
        document(before_registration),
        document(after_registration),
    )
    if (
        old.get("schema_version") != "workflow_host_control.v1"
        or not isinstance(old.get("phase"), str)
        or not isinstance(new.get("phase"), str)
        or (old.get("phase"), new.get("phase"))
        not in {
            ("DRAINING", "LEGACY_WRITERS_DISABLED"),
            ("LEGACY_WRITERS_DISABLED", "ACTIVE"),
        }
        or {**old, "phase": new.get("phase")} != new
        or old_registration.get("state_sha256") != hashlib.sha256(before_state).hexdigest()
        or new_registration.get("state_sha256") != hashlib.sha256(after_state).hexdigest()
        or {**old_registration, "state_sha256": new_registration.get("state_sha256")}
        != new_registration
    ):
        _control_fail("CUTOVER_TRANSITION_BINDING")
    positions = (
        (before_state, before_registration, "BEFORE_PUBLICATION"),
        (after_state, before_registration, "STATE_PUBLISHED"),
        (after_state, after_registration, "REGISTRATION_PUBLISHED"),
    )
    for state, registration, position in positions:
        if observed_state == state and observed_registration == registration:
            return position
    _control_fail("CUTOVER_PUBLICATION_OBSERVATION_UNKNOWN")


def _control_state(root: Path, *, expected_sha256: str | None = None) -> dict[str, Any]:
    return _validate_control_state(
        _control_json(root / CONTROL_STATE_NAME, expected_sha256=expected_sha256), root
    )


FULL_EXECUTION_POLICY_KEY = "full_execution_policy"
# Owner decision DEVX-015 2026-09-24: the restricted worker account isolates test
# and Full execution only. Absent means the pre-decision coordinator Job model.
FULL_EXECUTION_POLICIES = frozenset({"COORDINATOR_JOB", "PROTECTED_WORKER_REQUIRED"})


def full_execution_policy(state: Mapping[str, Any]) -> str:
    """Declared Full isolation of a validated host control state."""
    value = state.get(FULL_EXECUTION_POLICY_KEY, "COORDINATOR_JOB")
    if value not in FULL_EXECUTION_POLICIES:
        _control_fail("FULL_EXECUTION_POLICY")
    return str(value)


def _validate_control_state(state: dict[str, Any], root: Path) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.workflow_contract import portable_path

    if FULL_EXECUTION_POLICY_KEY in state:
        full_execution_policy(state)
    if (
        set(state) - {FULL_EXECUTION_POLICY_KEY}
        != {
            "schema_version",
            "phase",
            "host_id",
            "root_identity",
            "epoch",
            "registrations",
            "legacy_roots",
            "resource_markers",
            "policy_sha256",
        }
        or state.get("schema_version") != "workflow_host_control.v1"
        or not isinstance(state.get("phase"), str)
        or state["phase"] not in {"DRAINING", "LEGACY_WRITERS_DISABLED", "ACTIVE"}
        or state.get("host_id") != machine_host_id()
        or state.get("root_identity") != directory_identity(root)
    ):
        _control_fail("STATE_IDENTITY")
    if not isinstance(state.get("epoch"), str) or not state["epoch"]:
        _control_fail("EPOCH")
    if not isinstance(state.get("registrations"), list) or not isinstance(
        state.get("legacy_roots"), list
    ):
        _control_fail("STATE_INVENTORY")
    for name in ("registrations", "legacy_roots"):
        seen: set[str] = set()
        registration_ids: set[str] = set()
        for row in state[name]:
            expected = (
                {"registration_id", "common_root", "common_identity", "entrypoints"}
                if name == "registrations"
                else {"root_identity", "epoch"}
            )
            if not isinstance(row, dict) or set(row) != expected:
                _control_fail("STATE_INVENTORY_ROW")
            identity = row["common_identity" if name == "registrations" else "root_identity"]
            _validate_directory_identity(identity)
            key = identity["path"].casefold()
            if key in seen:
                _control_fail("STATE_INVENTORY_DUPLICATE")
            seen.add(key)
            if name == "registrations":
                if (
                    row["common_root"] != identity["path"]
                    or not isinstance(row["registration_id"], str)
                    or not row["registration_id"]
                    or not isinstance(row["entrypoints"], list)
                    or not row["entrypoints"]
                    or any(not isinstance(item, str) or not item for item in row["entrypoints"])
                    or len(set(row["entrypoints"])) != len(row["entrypoints"])
                ):
                    _control_fail("STATE_REGISTRATION")
                if row["registration_id"] in registration_ids:
                    _control_fail("STATE_REGISTRATION_DUPLICATE")
                registration_ids.add(row["registration_id"])
            elif row["epoch"] != state["epoch"]:
                _control_fail("RETIREMENT_EPOCH")
    markers = state["resource_markers"]
    if not isinstance(markers, dict) or set(markers) != {"full", "publication"}:
        _control_fail("RESOURCE_MARKERS")
    for marker in markers.values():
        portable_path(marker)
    if markers["full"].casefold() == markers["publication"].casefold():
        _control_fail("RESOURCE_MARKERS")
    digest = state["policy_sha256"]
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
    ):
        _control_fail("POLICY_IDENTITY")
    return state


def _validate_directory_identity(value: Any) -> None:
    if (
        not isinstance(value, dict)
        or set(value) != {"path", "device", "file_id"}
        or not isinstance(value["path"], str)
        or not Path(value["path"]).is_absolute()
        or type(value["device"]) is not int
        or type(value["file_id"]) is not int
    ):
        _control_fail("DIRECTORY_IDENTITY_SCHEMA")


def control_policy_sha256(policy: Any) -> str:
    from dataclasses import asdict

    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest

    return canonical_digest(asdict(policy))


@dataclass(frozen=True)
class HostControlBinding:
    project_root: Path
    common_root: Path
    root: Path
    host_id: str
    epoch: str
    registration_id: str
    locator_sha256: str
    root_identity: Mapping[str, Any]
    entrypoint: str
    project_identity: Mapping[str, Any]
    common_identity: Mapping[str, Any]
    resource_markers: Mapping[str, str]
    policy_sha256: str

    def assert_current(self, *, operation: str) -> dict[str, Any]:
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

        registration = _trusted_host_registration()
        if registration is None:
            _control_fail("HOST_REGISTRATION_REQUIRED")
        authority = _trusted_repository(
            registration,
            project=self.project_identity,
            common=self.common_identity,
        )
        if (
            registration["root_identity"] != self.root_identity
            or registration["control_root"] != self.root.as_posix()
            or authority["locator_sha256"] != self.locator_sha256
        ):
            _control_fail("HOST_REGISTRATION_CHANGED")
        content = bounded_regular_bytes(self.common_root / CONTROL_BINDING_NAME, budget=65536)
        if hashlib.sha256(content).hexdigest() != self.locator_sha256:
            _control_fail("REGISTRATION_CHANGED")
        if (
            directory_identity(self.project_root) != self.project_identity
            or directory_identity(self.common_root) != self.common_identity
        ):
            _control_fail("REPOSITORY_IDENTITY_CHANGED")
        state = _control_state(self.root, expected_sha256=registration["state_sha256"])
        if (
            state["epoch"] != self.epoch
            or state["host_id"] != self.host_id
            or state["root_identity"] != self.root_identity
            or state["resource_markers"] != self.resource_markers
            or state["policy_sha256"] != self.policy_sha256
        ):
            _control_fail("WRITER_EPOCH_FENCED")
        matches = [
            row
            for row in state["registrations"]
            if row.get("registration_id") == self.registration_id
        ]
        if (
            len(matches) != 1
            or matches[0].get("common_root") != self.common_root.as_posix()
            or matches[0].get("common_identity") != self.common_identity
            or self.entrypoint not in matches[0].get("entrypoints", [])
        ):
            _control_fail("ENTRYPOINT_NOT_REGISTERED")
        if state["phase"] != "ACTIVE" and operation not in _DRAIN_OPERATIONS:
            _control_fail("MIGRATION_DRAINING")
        return state

    def scoped_path(self, path: str) -> str:
        scopes = self.scoped_paths(path)
        if len(scopes) != 1:
            _control_fail("RESOURCE_SCOPE_REQUIRES_MULTIPLE")
        return scopes[0]

    def scoped_paths(self, path: str) -> tuple[str, ...]:
        """Keep ordinary path exclusion plus every intersecting reserved scope.

        Broad parents cannot silently lose their ordinary-path claim or one of
        several contained reserved resources. Windows case variants are equal.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            canonical_digest,
            portable_path,
        )

        portable_path(path)
        state = self.assert_current(operation="observe")
        markers = state["resource_markers"]
        resources = {
            "full": "host/" + self.host_id + "/full",
            "publication": "repository/" + canonical_digest(self.common_identity) + "/publication",
        }
        normalized = path.casefold()
        scopes: set[str] = set()
        exact_marker = False
        for kind, marker in markers.items():
            target = marker.casefold()
            if normalized == target:
                exact_marker = True
                scopes.add(resources[kind])
            elif normalized.startswith(target + "/") or target.startswith(normalized + "/"):
                scopes.add(resources[kind])
        if not exact_marker:
            scopes.add(
                "checkout/" + canonical_digest(self.project_identity) + "/path/" + normalized
            )
        return tuple(sorted(scopes))


def resolve_host_control_binding(
    project_root: Path, *, entrypoint: str
) -> HostControlBinding | None:
    """Resolve a registered common-Git locator, independent of checkout config.

    Only an entirely unregistered host may retain legacy behavior. A registered
    host must resolve an enrolled physical checkout/common pair even when Git or
    its locator is missing. Installation never infers trust from locator bytes.
    """
    import subprocess

    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    root = project_root.absolute()
    project_identity = directory_identity(root)
    registration = _trusted_host_registration()
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_OPTIONAL_LOCKS="0"
    )
    observed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--path-format=absolute", "--git-common-dir"],
        capture_output=True,
        text=True,
        env=environment,
        timeout=30,
    )
    if observed.returncode:
        if registration is not None or (root / ".git").exists():
            _control_fail("GIT_IDENTITY_UNAVAILABLE")
        # Synthetic non-Git stores before host enrolment are not managed entrypoints.
        return None
    common = Path(observed.stdout.strip()).absolute()
    common_identity = directory_identity(common)
    if registration is not None:
        _trusted_repository(registration, project=project_identity, common=common_identity)
    locator = common / CONTROL_BINDING_NAME
    if not locator.exists():
        if locator.is_symlink():
            _control_fail("REGISTRATION_REPARSE")
        if registration is not None:
            _control_fail("REGISTERED_LOCATOR_MISSING")
        return None
    if registration is None:
        _control_fail("HOST_REGISTRATION_REQUIRED")
    content = bounded_regular_bytes(locator, budget=65536)
    value = load_strict_json_text(content.decode("utf-8"))
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "common_root",
            "control_root",
            "root_identity",
            "host_id",
            "epoch",
            "registration_id",
            "common_identity",
            "resource_markers",
            "policy_sha256",
        }
        or value["schema_version"] != "workflow_host_binding.v1"
    ):
        _control_fail("REGISTRATION_SCHEMA")
    if (
        value["common_root"] != common.as_posix()
        or value["host_id"] != machine_host_id()
        or value["common_identity"] != common_identity
        or not isinstance(value["control_root"], str)
        or not Path(value["control_root"]).is_absolute()
    ):
        _control_fail("REGISTRATION_IDENTITY")
    binding = HostControlBinding(
        root,
        common,
        Path(value["control_root"]),
        value["host_id"],
        value["epoch"],
        value["registration_id"],
        hashlib.sha256(content).hexdigest(),
        value["root_identity"],
        entrypoint,
        project_identity,
        common_identity,
        value["resource_markers"],
        value["policy_sha256"],
    )
    binding.assert_current(operation="observe")
    return binding


def assert_store_writer(store: FileExecutionLeaseStore, *, operation: str) -> None:
    """Called while the existing store OS arbiter is held, for EVERY write."""
    if store.requested_root != store.root:
        _control_fail("STORE_ALIAS")
    if store.coordination_binding is not None:
        binding = store.coordination_binding
        if not isinstance(binding, HostControlBinding) or store.root != binding.root:
            _control_fail("STORE_BINDING")
        binding.assert_current(operation=operation)
        if control_policy_sha256(store.policy) != binding.policy_sha256:
            _control_fail("STORE_POLICY_CHANGED")
    elif (store.root / CONTROL_STATE_NAME).exists():
        _control_fail("REGISTERED_BINDING_REQUIRED")
    registration = _trusted_host_registration()
    trusted_state = None
    if registration is not None and store.coordination_binding is None:
        trusted_root = Path(registration["control_root"])
        if store.root == trusted_root:
            _control_fail("REGISTERED_BINDING_REQUIRED")
        trusted_state = _control_state(
            trusted_root,
            expected_sha256=registration["state_sha256"],
        )
        legacy_rows = [
            row
            for row in trusted_state["legacy_roots"]
            if Path(row["root_identity"]["path"]) == store.root
        ]
        if not legacy_rows:
            _control_fail("RETIREMENT_NOT_REGISTERED")
        if legacy_rows and not (store.root / RETIREMENT_NAME).exists():
            _control_fail("LEGACY_FENCE_MISSING")
    retirement = store.root / RETIREMENT_NAME
    if retirement.exists():
        marker = _control_json(retirement)
        if (
            set(marker) != {"schema_version", "root_identity", "control_root", "epoch"}
            or marker.get("schema_version") != "workflow_legacy_retirement.v1"
            or marker.get("root_identity") != directory_identity(store.root)
        ):
            _control_fail("RETIREMENT_IDENTITY")
        if registration is None or marker["control_root"] != registration["control_root"]:
            _control_fail("RETIREMENT_HOST_NOT_REGISTERED")
        state = trusted_state or _control_state(
            Path(marker["control_root"]),
            expected_sha256=registration["state_sha256"],
        )
        if (
            marker["epoch"] != state["epoch"]
            or {"root_identity": marker["root_identity"], "epoch": marker["epoch"]}
            not in state["legacy_roots"]
        ):
            _control_fail("RETIREMENT_NOT_REGISTERED")
        if state["phase"] != "DRAINING" or operation not in _DRAIN_OPERATIONS:
            _control_fail("LEGACY_WRITER_RETIRED")


def coordinated_lease_store(
    project_root: Path, legacy_root: Path, *, policy: Any, entrypoint: str
) -> FileExecutionLeaseStore:
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        FileExecutionLeaseStore,
    )

    binding = resolve_host_control_binding(project_root, entrypoint=entrypoint)
    store = FileExecutionLeaseStore(
        binding.root if binding is not None else legacy_root, policy=policy
    )
    store.coordination_binding = binding
    return store


def inspect_host_control(project_root: Path) -> dict[str, Any]:
    """Read-only public inspection. Registration is not migration acceptance."""
    from ai_trading_system.platform.architecture.workflow_contract import repository_identity

    identity = repository_identity(project_root)
    registration = _trusted_host_registration()
    if registration is None:
        # A present untrusted locator is damage, not permission to retain legacy.
        binding = resolve_host_control_binding(project_root, entrypoint="checkout-guard")
        if binding is not None:
            _control_fail("HOST_REGISTRATION_CHANGED")
        return {
            "schema_version": "workflow_host_inspection.v1",
            "status": "NOT_ENROLLED",
            "repository": identity,
            "host_id": machine_host_id(),
            "control_root": None,
            "migration_accepted": False,
            "mutation_performed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    binding = resolve_host_control_binding(project_root, entrypoint="checkout-guard")
    if binding is None:
        _control_fail("HOST_REGISTRATION_CHANGED")
    state = binding.assert_current(operation="observe")
    return {
        "schema_version": "workflow_host_inspection.v1",
        "status": "REGISTERED",
        "repository": identity,
        "host_id": binding.host_id,
        "control_root": binding.root.as_posix(),
        "phase": state["phase"],
        "epoch": state["epoch"],
        "registered_repository_count": len(registration["repositories"]),
        "legacy_root_count": len(state["legacy_roots"]),
        "migration_accepted": False,
        "mutation_performed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def registered_legacy_terminal_lease(
    project_root: Path, legacy_root: Path, *, policy: Any, lease_id: str
) -> ExecutionLease | None:
    """Read a terminal origin, never attach an old store as writer authority.

    The caller supplies its policy-derived original runtime root, not a path
    from a receipt. No other registered root is searched for a matching id.
    """
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        parse_lease_event,
        replay_lease_events,
    )
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    binding = resolve_host_control_binding(project_root, entrypoint="checkout-guard")
    if binding is None or legacy_root.absolute() == binding.root:
        return None
    state = binding.assert_current(operation="observe")
    if control_policy_sha256(policy) != binding.policy_sha256:
        _control_fail("STORE_POLICY_CHANGED")
    rows = [
        row
        for row in state["legacy_roots"]
        if row["root_identity"]["path"] == legacy_root.as_posix()
    ]
    if len(rows) != 1:
        _control_fail("TERMINAL_ORIGIN_NOT_REGISTERED")
    identity = rows[0]["root_identity"]
    if directory_identity(legacy_root) != identity:
        _control_fail("TERMINAL_ORIGIN_CHANGED")
    expected_marker = {
        "schema_version": "workflow_legacy_retirement.v1",
        "root_identity": identity,
        "control_root": binding.root.as_posix(),
        "epoch": state["epoch"],
    }
    if _control_json(legacy_root / RETIREMENT_NAME) != expected_marker:
        _control_fail("RETIREMENT_NOT_REGISTERED")
    # Reuse the canonical event parser/replay, with bounded native reads. Do not
    # follow an event-directory junction or silently drop an unexpected member.
    events_root = legacy_root / "events"
    events_identity = directory_identity(events_root)
    events = []
    buckets = sorted(events_root.iterdir())
    for bucket in buckets:
        bucket_identity = directory_identity(bucket)
        paths = sorted(bucket.iterdir())
        for path in paths:
            payload = load_strict_json_text(bounded_regular_bytes(path).decode("utf-8"))
            if not isinstance(payload, dict):
                _control_fail("TERMINAL_ORIGIN_EVENT_SCHEMA")
            event = parse_lease_event(payload)
            if bucket.name != event.lease.lease_id or path.name != event.event_id + ".json":
                _control_fail("TERMINAL_ORIGIN_EVENT_PATH")
            events.append(event)
        if directory_identity(bucket) != bucket_identity or sorted(bucket.iterdir()) != paths:
            _control_fail("TERMINAL_ORIGIN_CHANGED")
    replay = replay_lease_events(events)
    if replay.status != "PASS":
        _control_fail("TERMINAL_ORIGIN_REPLAY_INVALID")
    if (
        directory_identity(events_root) != events_identity
        or sorted(events_root.iterdir()) != buckets
        or directory_identity(legacy_root) != identity
        or _control_json(legacy_root / RETIREMENT_NAME) != expected_marker
        or binding.assert_current(operation="observe") != state
    ):
        _control_fail("TERMINAL_ORIGIN_CHANGED")
    lease = next((head for head in replay.lease_heads if head.lease_id == lease_id), None)
    if lease is not None and (
        lease.state != "RELEASED" or not execution_is_terminal(lease.execution)
    ):
        _control_fail("TERMINAL_ORIGIN_NOT_TERMINAL")
    return lease


def _cutover_execution_observations(execution: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Check recorded executors in the OS, including nested execution attempts.

    A terminal event is not itself proof that its process/children are absent.
    This does not inventory unregistered old binaries or enforce their OS fence.
    """
    from ai_trading_system.platform.architecture.workflow_execution import (
        observe_job,
        observe_process,
    )

    def walk(item: Mapping[str, Any]):
        yield item
        for collection in ("installation_attempts", "publication_attempts"):
            for attempt in item.get(collection, ()):
                yield from walk(attempt)

    observations: list[dict[str, Any]] = []
    for item in walk(execution):
        job = observe_job(item["request"]["job_name"])
        process = item["process"]
        observed = None if process is None else observe_process(**process)
        if job["state"] not in {"EMPTY", "ABSENT"} or (
            observed is not None and observed["state"] not in {"EXITED", "REUSED"}
        ):
            _control_fail("CUTOVER_EXECUTOR_NOT_DRAINED")
        observations.append(
            {"request_id": item["request"]["request_id"], "job": job, "process": observed}
        )
    return observations


@dataclass(frozen=True)
class HostCutoverCustody:
    """Short-lived existing-arbiter custody, NOT a migration activation receipt.

    No serialized input can keep these handles alive or authorize registry/ACL
    writes. Administrator cutover must additionally enforce the old-binary fence
    and its durable recovery protocol before changing the trusted registration.
    """

    binding: HostControlBinding
    state: Mapping[str, Any]
    root_identities: tuple[Mapping[str, Any], ...]
    lease_snapshots: tuple[Mapping[str, Any], ...]
    _handles: tuple[Any, ...]

    def assert_current(self) -> None:
        for held, identity in zip(self._handles, self.root_identities, strict=True):
            held.assert_owner(owner_token=held.token)
            held.assert_anchor()
            if directory_identity(Path(identity["path"])) != identity:
                _control_fail("CUTOVER_ROOT_CHANGED")
        if self.binding.assert_current(operation="observe") != self.state:
            _control_fail("CUTOVER_STATE_CHANGED")


@contextmanager
def host_cutover_custody(
    project_root: Path, *, policy: Any, actor: str
) -> Iterator[HostCutoverCustody]:
    """Hold all existing old/new arbiters in physical-root order for cutover.

    Never wait for children, expire/release a business lease, copy events, change
    a phase, or treat this barrier alone as permission to activate. Entry failure
    unwinds acquired OS handles. Arbiter owner diagnostics are the only writes.
    """
    from ai_trading_system.platform.architecture.lease_arbiter import hold_lease_arbiter
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        FileExecutionLeaseStore,
    )

    if actor not in policy.allowlisted_actors:
        _control_fail("CUTOVER_ACTOR")
    binding = resolve_host_control_binding(project_root, entrypoint="checkout-guard")
    if binding is None:
        _control_fail("HOST_REGISTRATION_REQUIRED")
    state = binding.assert_current(operation="observe")
    if state["phase"] not in {"DRAINING", "LEGACY_WRITERS_DISABLED"}:
        _control_fail("CUTOVER_PHASE")
    if binding.policy_sha256 != control_policy_sha256(policy):
        _control_fail("STORE_POLICY_CHANGED")
    identities = [binding.root_identity, *(row["root_identity"] for row in state["legacy_roots"])]
    physical = {(row["device"], row["file_id"]) for row in identities}
    if len(physical) != len(identities):
        _control_fail("CUTOVER_ROOT_ALIAS")
    identities.sort(key=lambda row: (row["device"], row["file_id"]))
    roots = [Path(row["path"]) for row in identities]
    if any(
        first != second and (first.is_relative_to(second) or second.is_relative_to(first))
        for first in roots
        for second in roots
    ):
        _control_fail("CUTOVER_ROOT_OVERLAP")
    for root, identity in zip(roots, identities, strict=True):
        if directory_identity(root) != identity:
            _control_fail("CUTOVER_ROOT_CHANGED")
    with ExitStack() as stack:
        handles = tuple(
            stack.enter_context(
                hold_lease_arbiter(
                    root,
                    actor=actor,
                    now=datetime.now(UTC),
                    arbiter_ttl_seconds=policy.arbiter_ttl_seconds,
                )
            )
            for root in roots
        )
        custody = HostCutoverCustody(binding, state, tuple(identities), (), handles)
        custody.assert_current()
        snapshots: list[Mapping[str, Any]] = []
        for root, identity in zip(roots, identities, strict=True):
            if root != binding.root:
                marker = _control_json(root / RETIREMENT_NAME)
                if marker != {
                    "schema_version": "workflow_legacy_retirement.v1",
                    "root_identity": identity,
                    "control_root": binding.root.as_posix(),
                    "epoch": state["epoch"],
                }:
                    _control_fail("RETIREMENT_NOT_REGISTERED")
            replay = FileExecutionLeaseStore(root, policy=policy).replay()
            if replay.status != "PASS":
                _control_fail("CUTOVER_REPLAY_INVALID")
            if any(
                head.state == "ACTIVE" or not execution_is_terminal(head.execution)
                for head in replay.lease_heads
            ):
                _control_fail("CUTOVER_DRAIN_REQUIRED")
            observations = [
                {
                    "lease_id": head.lease_id,
                    "executions": _cutover_execution_observations(head.execution),
                }
                for head in replay.lease_heads
                if head.execution is not None
            ]
            snapshots.append(
                {
                    "root_identity": identity,
                    "event_count": replay.event_count,
                    "head_event_ids": dict(replay.head_event_ids),
                    "executors": observations,
                }
            )
        custody = replace(custody, lease_snapshots=tuple(snapshots))
        custody.assert_current()
        try:
            yield custody
        finally:
            # The eventual administrator transition owns expected state changes;
            # these original handles/physical roots must remain owned regardless.
            for held, identity in zip(handles, identities, strict=True):
                held.assert_owner(owner_token=held.token)
                held.assert_anchor()
                if directory_identity(Path(identity["path"])) != identity:
                    _control_fail("CUTOVER_ROOT_CHANGED")


@dataclass(frozen=True)
class _CutoverRecoveryCustody:
    """Live recovery barrier only; never a receipt authorizing activation."""

    root: Path
    documents: Mapping[str, bytes]
    root_identities: tuple[Mapping[str, Any], ...]
    lease_snapshots: tuple[Mapping[str, Any], ...]
    _handles: tuple[Any, ...]
    _lifetime: list[bool]

    def assert_current(self) -> str:
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

        if not self._lifetime[0]:
            _control_fail("CUTOVER_RECOVERY_CLOSED")
        for held, identity in zip(self._handles, self.root_identities, strict=True):
            held.assert_owner(owner_token=held.token)
            held.assert_anchor()
            if directory_identity(Path(identity["path"])) != identity:
                _control_fail("CUTOVER_ROOT_CHANGED")
        registration = _trusted_host_registration_bytes()
        if registration is None:
            _control_fail("HOST_REGISTRATION_REQUIRED")
        return _cutover_publication_position(
            **self.documents,
            observed_state=bounded_regular_bytes(
                self.root / CONTROL_STATE_NAME, budget=1024 * 1024
            ),
            observed_registration=registration,
        )


@contextmanager
def host_cutover_recovery_custody(
    control_root: Path,
    *,
    journal_sha256: str,
    policy: Any,
    actor: str,
) -> Iterator[_CutoverRecoveryCustody]:
    """Admit a journal-bound interrupted pair without relaxing normal writers.

    Acquires only the original arbiters. Existing roots and retired markers,
    complete lease replay and native execution quiescence remain mandatory.
    No state/registration/ACL write, lease expiry, copying or activation occurs.
    """
    from types import MappingProxyType

    from ai_trading_system.platform.architecture.lease_arbiter import hold_lease_arbiter
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        FileExecutionLeaseStore,
    )
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    if actor not in policy.allowlisted_actors:
        _control_fail("CUTOVER_ACTOR")
    administrator = _WindowsEnrollmentAdministrator()
    registration = _trusted_host_registration_bytes()
    if registration is None:
        _control_fail("HOST_REGISTRATION_REQUIRED")
    registered = load_strict_json_text(registration.decode("utf-8"))
    if not control_root.is_absolute() or control_root.as_posix() != registered["control_root"]:
        _control_fail("CUTOVER_JOURNAL_ROOT")
    with administrator.hold_cutover_journal(control_root, journal_sha256) as original:
        documents = MappingProxyType(dict(original))
        state = load_strict_json_text(documents["before_state"].decode("utf-8"))
        if state["policy_sha256"] != control_policy_sha256(policy):
            _control_fail("STORE_POLICY_CHANGED")
        _cutover_publication_position(
            **documents,
            observed_state=bounded_regular_bytes(
                control_root / CONTROL_STATE_NAME, budget=1024 * 1024
            ),
            observed_registration=registration,
        )
        identities = [
            state["root_identity"],
            *(row["root_identity"] for row in state["legacy_roots"]),
        ]
        if len({(row["device"], row["file_id"]) for row in identities}) != len(identities):
            _control_fail("CUTOVER_ROOT_ALIAS")
        identities.sort(key=lambda row: (row["device"], row["file_id"]))
        roots = [Path(row["path"]) for row in identities]
        if any(
            a != b and (a.is_relative_to(b) or b.is_relative_to(a)) for a in roots for b in roots
        ):
            _control_fail("CUTOVER_ROOT_OVERLAP")
        # Check existence/identity before hold_lease_arbiter can create anything.
        for root, identity in zip(roots, identities, strict=True):
            if directory_identity(root) != identity:
                _control_fail("CUTOVER_ROOT_CHANGED")
        with administrator.pin_directories(tuple(roots)), ExitStack() as stack:
            handles = tuple(
                stack.enter_context(
                    hold_lease_arbiter(
                        root,
                        actor=actor,
                        now=datetime.now(UTC),
                        arbiter_ttl_seconds=policy.arbiter_ttl_seconds,
                    )
                )
                for root in roots
            )
            lifetime = [True]
            custody = _CutoverRecoveryCustody(
                control_root,
                documents,
                tuple(MappingProxyType(dict(row)) for row in identities),
                (),
                handles,
                lifetime,
            )
            try:
                custody.assert_current()
                snapshots = []
                for root, identity in zip(roots, identities, strict=True):
                    if root != control_root and _control_json(root / RETIREMENT_NAME) != {
                        "schema_version": "workflow_legacy_retirement.v1",
                        "root_identity": identity,
                        "control_root": control_root.as_posix(),
                        "epoch": state["epoch"],
                    }:
                        _control_fail("RETIREMENT_NOT_REGISTERED")
                    replay = FileExecutionLeaseStore(root, policy=policy).replay()
                    if replay.status != "PASS":
                        _control_fail("CUTOVER_REPLAY_INVALID")
                    if any(
                        head.state == "ACTIVE" or not execution_is_terminal(head.execution)
                        for head in replay.lease_heads
                    ):
                        _control_fail("CUTOVER_DRAIN_REQUIRED")
                    snapshots.append(
                        {
                            "root_identity": identity,
                            "event_count": replay.event_count,
                            "head_event_ids": dict(replay.head_event_ids),
                            "executors": [
                                {
                                    "lease_id": head.lease_id,
                                    "executions": _cutover_execution_observations(head.execution),
                                }
                                for head in replay.lease_heads
                                if head.execution is not None
                            ],
                        }
                    )
                custody = replace(custody, lease_snapshots=tuple(snapshots))
                custody.assert_current()
                yield custody
            finally:
                lifetime[0] = False
                for held, identity in zip(handles, identities, strict=True):
                    held.assert_owner(owner_token=held.token)
                    held.assert_anchor()
                    if directory_identity(Path(identity["path"])) != identity:
                        _control_fail("CUTOVER_ROOT_CHANGED")


def plan_host_enrollment(
    project_root: Path,
    *,
    policy: Any,
    control_root: Path,
    legacy_roots: list[Path],
    additional_repositories: list[Path] | None = None,
) -> dict[str, Any]:
    """Inventory first enrollment without creating roots or granting cutover.

    This non-atomic observation is an installation review input. Administrator
    execution must independently establish OS protection, quiescence, current
    identities and durable recovery; a plan digest grants none of those rights.
    """
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        FileExecutionLeaseStore,
    )
    from ai_trading_system.platform.architecture.workflow_contract import (
        canonical_digest,
        repository_identity,
    )

    if _trusted_host_registration() is not None:
        _control_fail("HOST_ALREADY_ENROLLED")
    if not legacy_roots:
        _control_fail("ENROLLMENT_LEGACY_INVENTORY_REQUIRED")
    if any(not path.is_absolute() for path in (control_root, *legacy_roots)):
        _control_fail("ENROLLMENT_ABSOLUTE_PATH_REQUIRED")
    target = control_root.absolute()
    parent_identity = directory_identity(target.parent)
    target_identity = directory_identity(target) if target.exists() else None
    if target.is_symlink() or target != target.resolve():
        _control_fail("ROOT_ALIAS")
    repositories = []
    for root in [project_root, *(additional_repositories or [])]:
        physical = directory_identity(root)
        identity = repository_identity(root)
        repositories.append(
            {
                "repository": identity,
                "checkout_identity": physical,
                "common_identity": directory_identity(Path(identity["common"])),
            }
        )
    checkout_ids = [
        (row["checkout_identity"]["device"], row["checkout_identity"]["file_id"])
        for row in repositories
    ]
    if len(set(checkout_ids)) != len(checkout_ids):
        _control_fail("HOST_CHECKOUT_DUPLICATE")
    identities = [directory_identity(root) for root in legacy_roots]
    roots = [Path(row["path"]) for row in identities]
    physical_ids = [(row["device"], row["file_id"]) for row in identities]
    if len(set(physical_ids)) != len(physical_ids):
        _control_fail("CUTOVER_ROOT_ALIAS")
    all_roots = [target, _enrollment_preparation_path(target), *roots]
    if any(
        first.is_relative_to(second) or second.is_relative_to(first)
        for index, first in enumerate(all_roots)
        for second in all_roots[index + 1 :]
    ):
        _control_fail("CUTOVER_ROOT_OVERLAP")
    for row in repositories:
        for name in ("checkout_identity", "common_identity"):
            repository_root = Path(row[name]["path"])
            if any(
                proposed.is_relative_to(repository_root) or repository_root.is_relative_to(proposed)
                for proposed in (target, _enrollment_preparation_path(target))
            ):
                _control_fail("ENROLLMENT_TARGET_REPOSITORY_OVERLAP")
    if target_identity is not None and any(target.iterdir()):
        _control_fail("ENROLLMENT_TARGET_NOT_EMPTY")
    snapshots = []
    for root, identity in zip(roots, identities, strict=True):
        replay = FileExecutionLeaseStore(root, policy=policy).replay()
        if replay.status != "PASS":
            _control_fail("CUTOVER_REPLAY_INVALID")
        active = sorted(head.lease_id for head in replay.lease_heads if head.state == "ACTIVE")
        unresolved = sorted(
            head.lease_id
            for head in replay.lease_heads
            if not execution_is_terminal(head.execution)
        )
        executors = []
        for head in replay.lease_heads:
            if head.execution is None:
                continue
            try:
                observations = _cutover_execution_observations(head.execution)
            except ParallelControlError as exc:
                if exc.code != "WORKFLOW_CONTROL_CUTOVER_EXECUTOR_NOT_DRAINED":
                    raise
                executors.append({"lease_id": head.lease_id, "drained": False})
            else:
                executors.append(
                    {
                        "lease_id": head.lease_id,
                        "drained": True,
                        "observations": observations,
                    }
                )
        if directory_identity(root) != identity:
            _control_fail("CUTOVER_ROOT_CHANGED")
        snapshots.append(
            {
                "root_identity": identity,
                "event_count": replay.event_count,
                "head_event_ids": dict(replay.head_event_ids),
                "active_lease_ids": active,
                "nonterminal_execution_lease_ids": unresolved,
                "executors": executors,
            }
        )
    if directory_identity(target.parent) != parent_identity or (
        (directory_identity(target) if target.exists() else None) != target_identity
    ):
        _control_fail("CUTOVER_ROOT_CHANGED")
    if _trusted_host_registration() is not None:
        _control_fail("HOST_REGISTRATION_CHANGED")
    drain_required = any(
        row["active_lease_ids"]
        or row["nonterminal_execution_lease_ids"]
        or any(not item["drained"] for item in row["executors"])
        for row in snapshots
    )
    result = {
        "schema_version": "workflow_host_enrollment_plan.v1",
        "status": "DRAIN_REQUIRED" if drain_required else "ADMIN_INSTALLATION_REQUIRED",
        "host_id": machine_host_id(),
        "policy_sha256": control_policy_sha256(policy),
        "repositories": repositories,
        "target": {
            "path": target.as_posix(),
            "parent_identity": parent_identity,
            "existing_identity": target_identity,
            "preparation_path": _enrollment_preparation_path(target).as_posix(),
        },
        "legacy_stores": snapshots,
        "inventory_scope": "CALLER_DECLARED_NOT_HOST_EXHAUSTIVE",
        "snapshot_atomic": False,
        "remaining_requirements": [
            "COMPLETE_PARTICIPANT_AND_ENTRYPOINT_INVENTORY",
            "ADMINISTRATOR_PROTECTION",
            "OLD_ENTRYPOINT_OS_FENCE",
            "REAL_EXECUTOR_INVENTORY",
            "LOCKED_IDENTITY_AND_DRAIN_RECHECK",
            "DURABLE_CUTOVER_AND_RECOVERY",
        ],
        "activation_allowed": False,
        "mutation_performed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {**result, "plan_sha256": canonical_digest(result)}


ENROLLMENT_JOURNAL_NAME = "host-enrollment.v1.json"


def _enrollment_preparation_path(root: Path) -> Path:
    digest = hashlib.sha256(root.as_posix().casefold().encode("utf-8")).hexdigest()
    return root.parent / (".aits-enrollment-" + digest)


class WindowsWorkerExchange:
    """Live, single-use administrative exchange; serialized paths are not authority."""

    _administrator: _WindowsEnrollmentAdministrator
    _root: Path
    _worker_token: Any
    _identities: dict[str, Any]
    _owner: tuple[int, int]
    _used: bool

    def __init__(self) -> None:
        _control_fail("EXCHANGE_FACTORY_REQUIRED")

    @classmethod
    def create(cls, root: Path, worker_token: Any) -> WindowsWorkerExchange:
        administrator = _WindowsEnrollmentAdministrator()
        identities = administrator.create_worker_exchange(root, worker_token)
        instance = object.__new__(cls)
        instance._administrator, instance._root = administrator, root
        instance._worker_token, instance._identities = worker_token, identities
        instance._owner = (os.getpid(), threading.get_ident())
        instance._used = False
        return instance

    def validate_worker(self, worker_token: Any) -> None:
        if self._owner != (os.getpid(), threading.get_ident()):
            _control_fail("EXCHANGE_OWNER")
        if (worker_token is not self._worker_token
                or worker_token.validate_launcher() != self._identities["worker_identity"]):
            _control_fail("EXCHANGE_WORKER_CHANGED")

    @property
    def result_identity(self) -> tuple[int, int]:
        self.validate_worker(self._worker_token)
        value = self._identities["result_identity"]
        return int(value[0]), int(value[1])

    @property
    def profile_directory(self) -> Path:
        self._verify()
        return self._root / "profile"

    def _verify(self) -> None:
        self.validate_worker(self._worker_token)
        for role, path in (("root", self._root), ("result", self._root / "result.json"),
                           ("profile", self._root / "profile")):
            with self._administrator._security(
                worker_sid=str(self._identities["worker_identity"]["sid"]), exchange_role=role,
            ) as attributes:
                self._administrator._assert_exact_security(path, attributes.descriptor)
            if role == "result":
                info = path.stat()
                if (info.st_dev, info.st_ino) != self.result_identity or info.st_nlink != 1:
                    _control_fail("EXCHANGE_RESULT_CHANGED")
            elif directory_identity(path) != self._identities[role]:
                _control_fail("EXCHANGE_DIRECTORY_CHANGED")

    @contextmanager
    def directory(self) -> Iterator[Path]:
        self.validate_worker(self._worker_token)
        if self._used:
            _control_fail("EXCHANGE_ALREADY_USED")
        self._used = True
        with self._administrator.pin_directories((self._root, self.profile_directory)):
            self._verify()
            try:
                yield self._root
            finally:
                self._verify()  # Retain files; cleanup requires confirmed process exit.


def _worker_exchange_sddl(worker_sid: str, role: str) -> str:
    """Fixed exchange ACLs, not a caller-supplied permission policy."""
    if not isinstance(worker_sid, str) or not re.fullmatch(
        r"S-1-5-21-\d+-\d+-\d+-\d+", worker_sid,
    ):
        _control_fail("EXCHANGE_WORKER_SID")
    worker_rights = {
        "root": f"(A;OICI;0x1200a9;;;{worker_sid})",
        "result": f"(A;;0x12019f;;;{worker_sid})",
        # Only children can be replaced; no DELETE, WRITE_DAC/OWNER or
        # WRITE_ATTRIBUTES on this directory itself.
        "profile": (f"(A;;0x1200af;;;{worker_sid})"
                    f"(A;OICIIO;0x1301bf;;;{worker_sid})"),
    }
    if role not in worker_rights:
        _control_fail("EXCHANGE_OBJECT_ROLE")
    return "O:BAG:BAD:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)" + worker_rights[role]


class _WindowsEnrollmentAdministrator:
    """Native administrator transport. Never elevates or accepts an admin boolean.

    New administrative objects are protected at creation, not by a later ACL
    repair. Runtime write grants and old-writer fencing belong to activation,
    which this DRAINING-only installer deliberately cannot perform.
    """

    def __init__(self) -> None:
        import ctypes as c
        from ctypes import wintypes as w

        if os.name != "nt" or c.sizeof(c.c_void_p) != 8:
            _control_fail("ADMIN_PLATFORM_UNSUPPORTED")
        self.c, self.w = c, w
        self.kernel = c.WinDLL("kernel32", use_last_error=True)
        self.advapi = c.WinDLL("advapi32", use_last_error=True)
        self._bind(self.kernel, "CloseHandle", [w.HANDLE], w.BOOL)
        self._bind(self.kernel, "LocalFree", [c.c_void_p], c.c_void_p)
        self._bind(self.kernel, "GetCurrentProcess", [], w.HANDLE)
        self._bind(
            self.advapi, "OpenProcessToken", [w.HANDLE, w.DWORD, c.POINTER(w.HANDLE)], w.BOOL
        )
        self._bind(
            self.advapi,
            "GetTokenInformation",
            [w.HANDLE, c.c_int, c.c_void_p, w.DWORD, c.POINTER(w.DWORD)],
            w.BOOL,
        )
        token, elevated, size = w.HANDLE(), w.DWORD(), w.DWORD()
        if not self.advapi.OpenProcessToken(
            self.kernel.GetCurrentProcess(), 0x0008, c.byref(token)
        ):
            raise c.WinError(c.get_last_error())
        try:
            if not self.advapi.GetTokenInformation(
                token, 20, c.byref(elevated), c.sizeof(elevated), c.byref(size)
            ):
                raise c.WinError(c.get_last_error())
            shell = c.WinDLL("shell32", use_last_error=True)
            self._bind(shell, "IsUserAnAdmin", [], w.BOOL)
            if elevated.value != 1 or not shell.IsUserAnAdmin():
                _control_fail("ADMINISTRATOR_REQUIRED")
        finally:
            self.kernel.CloseHandle(token)

    @staticmethod
    def _bind(library: Any, name: str, arguments: list[Any], result: Any) -> Any:
        function = getattr(library, name)
        function.argtypes, function.restype = arguments, result
        return function

    @contextmanager
    def _security(
        self, *, registry: bool = False, worker_sid: str | None = None,
        exchange_role: str | None = None, confidential: bool = False,
    ) -> Iterator[Any]:
        c, w = self.c, self.w

        class Attributes(c.Structure):
            _fields_ = [("length", w.DWORD), ("descriptor", c.c_void_p), ("inherit", w.BOOL)]

        descriptor = c.c_void_p()
        convert = self._bind(
            self.advapi,
            "ConvertStringSecurityDescriptorToSecurityDescriptorW",
            [w.LPCWSTR, w.DWORD, c.POINTER(c.c_void_p), c.c_void_p],
            w.BOOL,
        )
        sddl = (
            "O:BAG:BAD:P(A;CI;KA;;;SY)(A;CI;KA;;;BA)(A;CI;KR;;;AU)"
            if registry
            else "O:BAG:BAD:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;GRGX;;;AU)"
        )
        if confidential:
            if registry or worker_sid is not None or exchange_role is not None:
                _control_fail("CONFIDENTIAL_SECURITY_CONTEXT")
            # DPAPI current-user scope is not an elevation boundary. Ciphertext
            # therefore must not inherit the ordinary installation's AU read ACE.
            sddl = "O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)"
        if worker_sid is not None or exchange_role is not None:
            if registry or worker_sid is None or exchange_role is None:
                _control_fail("EXCHANGE_SECURITY_CONTEXT")
            sddl = _worker_exchange_sddl(worker_sid, exchange_role)
        if not convert(sddl, 1, c.byref(descriptor), None):
            raise c.WinError(c.get_last_error())
        try:
            yield Attributes(c.sizeof(Attributes), descriptor, False)
        finally:
            self.kernel.LocalFree(descriptor)

    def assert_protected(
        self,
        path: Path | str,
        *,
        registry: bool = False,
        parent: bool = False,
        allow_inheritance: bool = False,
    ) -> None:
        c, w = self.c, self.w
        owner, dacl, descriptor = c.c_void_p(), c.c_void_p(), c.c_void_p()
        get_info = self._bind(
            self.advapi,
            "GetNamedSecurityInfoW",
            [
                w.LPWSTR,
                c.c_int,
                w.DWORD,
                c.POINTER(c.c_void_p),
                c.c_void_p,
                c.POINTER(c.c_void_p),
                c.c_void_p,
                c.POINTER(c.c_void_p),
            ],
            w.DWORD,
        )
        code = get_info(
            str(path),
            4 if registry else 1,
            5,
            c.byref(owner),
            None,
            c.byref(dacl),
            None,
            c.byref(descriptor),
        )
        if code:
            raise c.WinError(code)
        sid_text = self._bind(
            self.advapi, "ConvertSidToStringSidW", [c.c_void_p, c.POINTER(w.LPWSTR)], w.BOOL
        )

        def sid(pointer: Any) -> str:
            text = w.LPWSTR()
            if not sid_text(pointer, c.byref(text)):
                raise c.WinError(c.get_last_error())
            try:
                return text.value
            finally:
                self.kernel.LocalFree(c.cast(text, c.c_void_p))

        try:
            trusted = {"S-1-5-18", "S-1-5-32-544"}
            if not owner.value or not dacl.value or sid(owner) not in trusted:
                _control_fail("ADMIN_OBJECT_NOT_PROTECTED")
            control, revision = c.c_ushort(), w.DWORD()
            get_control = self._bind(
                self.advapi,
                "GetSecurityDescriptorControl",
                [c.c_void_p, c.POINTER(c.c_ushort), c.POINTER(w.DWORD)],
                w.BOOL,
            )
            if not get_control(descriptor, c.byref(control), c.byref(revision)):
                raise c.WinError(c.get_last_error())
            if not parent and not allow_inheritance and not control.value & 0x1000:
                _control_fail("ADMIN_OBJECT_INHERITANCE_ENABLED")
            get_ace = self._bind(
                self.advapi, "GetAce", [c.c_void_p, w.DWORD, c.POINTER(c.c_void_p)], w.BOOL
            )
            count = c.c_ushort.from_address(dacl.value + 4).value
            # Parent creation rights are permissible; replacing a protected
            # child or rewriting the parent's DACL/owner is not.
            mutable = 0x100C0040 if parent else (0x500D0026 if registry else 0x500D0156)
            for index in range(count):
                ace = c.c_void_p()
                if not get_ace(dacl, index, c.byref(ace)):
                    raise c.WinError(c.get_last_error())
                kind, flags = (
                    c.c_ubyte.from_address(ace.value).value,
                    c.c_ubyte.from_address(ace.value + 1).value,
                )
                if kind not in {0, 1}:
                    _control_fail("ADMIN_ACL_UNSUPPORTED")
                if kind == 1 or (parent and flags & 0x08):
                    continue
                mask = c.c_uint32.from_address(ace.value + 4).value
                principal = sid(c.c_void_p(ace.value + 8))
                # SOFTWARE may carry an inheritable CREATOR_OWNER placeholder.
                # It is not a token principal; our child keys are created with
                # a protected DACL and cannot inherit this permission.
                if registry and allow_inheritance and flags & 0x02 and principal == "S-1-3-0":
                    continue
                if mask & mutable and principal not in trusted:
                    _control_fail("ADMIN_OBJECT_NOT_PROTECTED")
        finally:
            self.kernel.LocalFree(descriptor)

    @contextmanager
    def pin_directories(
        self, paths: Sequence[Path], *, deny_target_writers: bool = False,
    ) -> Iterator[None]:
        """Pin ancestors; optionally exclude existing write handles on targets.

        This sharing check does not replace recursive protected ACLs or file
        custody, and does not itself forbid creating children by path.
        """
        c, w = self.c, self.w

        class FileInformation(c.Structure):
            _fields_ = [
                ("attributes", w.DWORD),
                ("creation", w.FILETIME),
                ("access", w.FILETIME),
                ("write", w.FILETIME),
                ("volume", w.DWORD),
                ("size_high", w.DWORD),
                ("size_low", w.DWORD),
                ("links", w.DWORD),
                ("index_high", w.DWORD),
                ("index_low", w.DWORD),
            ]

        information = self._bind(
            self.kernel,
            "GetFileInformationByHandle",
            [w.HANDLE, c.POINTER(FileInformation)],
            w.BOOL,
        )
        create = self._bind(
            self.kernel,
            "CreateFileW",
            [w.LPCWSTR, w.DWORD, w.DWORD, c.c_void_p, w.DWORD, w.DWORD, w.HANDLE],
            w.HANDLE,
        )
        with ExitStack() as stack:
            parents = {entry for path in paths for entry in (path, *path.parents)}
            for path in sorted(parents, key=lambda item: (len(item.parts), str(item))):
                before = directory_identity(path)
                # Attribute-only handles do not participate in the Windows
                # read/write/delete sharing check. LIST_DIRECTORY is required
                # for omission of FILE_SHARE_DELETE to actually pin a directory.
                sharing = 1 if deny_target_writers and path in paths else 3
                handle = create(str(path), 0x81, sharing, None, 3, 0x02200000, None)
                if handle == c.c_void_p(-1).value:
                    raise c.WinError(c.get_last_error())
                stack.callback(self.kernel.CloseHandle, handle)
                observed = FileInformation()
                if not information(handle, c.byref(observed)):
                    raise c.WinError(c.get_last_error())
                if (
                    observed.attributes & 0x400
                    or not observed.attributes & 0x10
                    or (observed.volume, observed.index_high << 32 | observed.index_low)
                    != (before["device"], before["file_id"])
                    or directory_identity(path) != before
                ):
                    _control_fail("CUTOVER_ROOT_CHANGED")
            yield

    @contextmanager
    def hold_protected_files(
        self, expected_files: Mapping[Path, str], *,
        protected_directories: Sequence[Path] = (),
        expected_file_identities: Mapping[Path, tuple[int, int, int]] | None = None,
    ) -> Iterator[None]:
        """Hold declared bytes; callers separately prove manifest completeness.

        Additional protected directories cover absent configuration names. This
        transport never creates objects, repairs ACLs, or grants execution.
        """
        from ai_trading_system.platform.architecture.workflow_contract import (
            bounded_regular_bytes,
            hold_bound_read_file,
        )

        if not expected_files:
            _control_fail("PROTECTED_FILES_EMPTY")
        files = dict(expected_files)
        identities = dict(expected_file_identities or {})
        if set(identities) - set(files) or any(
            type(value) is not tuple or len(value) != 3
            or any(type(part) is not int or part < 0 for part in value) or value[2] < 1
            for value in identities.values()
        ):
            _control_fail("PROTECTED_FILES_IDENTITY")
        directories = set(protected_directories) | {path.parent for path in files}
        if any(not path.is_absolute() or ".." in path.parts for path in (*files, *directories)):
            _control_fail("PROTECTED_FILES_PATH")
        if any(not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
               for digest in files.values()):
            _control_fail("PROTECTED_FILES_DIGEST")
        with self.pin_directories(tuple(directories)), ExitStack() as stack:
            # Expected identities are local to this continuously pinned interval.
            # Every leaf hold still independently checks its actual root/leaf.
            parent_identities: dict[Path, dict[str, Any]] = {}
            for directory in directories:
                self.assert_protected(directory)
            for path, digest in files.items():
                self.assert_protected(path)
                info = path.stat()
                identity = (info.st_dev, info.st_ino)
                declared = identities.get(path)
                if declared is not None and (*identity, info.st_nlink) != declared:
                    _control_fail("PROTECTED_FILES_IDENTITY_CHANGED")
                links = declared[2] if declared is not None else 1
                # Installed tools/native libraries can exceed the generic
                # 16 MiB artifact default; retain the reader's 64 MiB hard cap.
                budget = min(info.st_size, 64 * 1024 * 1024)
                raw = bounded_regular_bytes(
                    path, expected_identity=identity, expected_link_count=links, budget=budget,
                )
                if hashlib.sha256(raw).hexdigest() != digest:
                    _control_fail("PROTECTED_FILES_CHANGED")
                if path.parent not in parent_identities:
                    parent_identities[path.parent] = directory_identity(path.parent)
                parent = parent_identities[path.parent]
                stack.enter_context(hold_bound_read_file(
                    path.parent, path.name, expected=raw, expected_identity=identity,
                    expected_root_identity=(parent["device"], parent["file_id"]),
                    expected_parent_identities={}, allow_parent_updates=True,
                    expected_link_count=links, budget=budget,
                ))
                self.assert_protected(path)
            try:
                yield
            finally:
                for path in (*files, *directories):
                    self.assert_protected(path)

    def _prepare_cutover_journal(self, custody: HostCutoverCustody) -> dict[str, Any]:
        """Persist one exact transition under original custody; never activate.

        Uses the existing administrative artifact writer's 1 MiB envelope cap.
        Oversized preparation fails before publication, without relaxing limits.
        The journal records intended bytes, not proof of an old-writer OS fence.
        """
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
        from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

        custody.assert_current()
        root = custody.binding.root
        self.assert_protected(root)
        before = bounded_regular_bytes(root / CONTROL_STATE_NAME, budget=1024 * 1024)
        registration = _trusted_host_registration_bytes()
        if registration is None:
            _control_fail("HOST_REGISTRATION_REQUIRED")
        state = _validate_control_state(load_strict_json_text(before.decode("utf-8")), root)
        anchor = _validate_host_registration(load_strict_json_text(registration.decode("utf-8")))
        if state != custody.state or anchor["control_root"] != root.as_posix():
            _control_fail("CUTOVER_STATE_CHANGED")
        target = {"DRAINING": "LEGACY_WRITERS_DISABLED", "LEGACY_WRITERS_DISABLED": "ACTIVE"}
        if state["phase"] not in target:
            _control_fail("CUTOVER_PHASE")

        def encode(value: Mapping[str, Any]) -> bytes:
            return (json.dumps(value, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")

        after = encode({**state, "phase": target[state["phase"]]})
        documents = {
            "before_state": before, "after_state": after,
            "before_registration": registration,
            "after_registration": encode(
                {**anchor, "state_sha256": hashlib.sha256(after).hexdigest()}
            ),
        }
        _cutover_publication_position(
            **documents, observed_state=before, observed_registration=registration,
        )
        payload = {"schema_version": "workflow_cutover_journal.v1",
                   **{name: raw.hex() for name, raw in documents.items()}}
        digest = hashlib.sha256(encode(payload)).hexdigest()
        path = root / ("cutover-" + digest + ".json")
        custody.assert_current()
        self.write_admin_json(path, payload)
        with self.hold_cutover_journal(root, digest) as observed:
            if observed != documents:
                _control_fail("CUTOVER_JOURNAL_CHANGED")
            custody.assert_current()
        return {"path": path.as_posix(), "sha256": digest, "activation_allowed": False}

    @contextmanager
    def hold_cutover_journal(self, root: Path, digest: str) -> Iterator[dict[str, bytes]]:
        """Read exact protected transition bytes; no mutation or activation grant.

        The digest must come from the reviewed operation. Holding this journal
        does not replace host cutover custody, writer fencing or live readback.
        """
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
        from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

        _control_digest(digest)
        if not root.is_absolute() or ".." in root.parts:
            _control_fail("CUTOVER_JOURNAL_PATH")
        path = root / ("cutover-" + digest + ".json")
        # Four <=1 MiB documents encoded as hex plus a small schema envelope.
        with self.hold_protected_files({path: digest}, protected_directories=(root,)):
            raw = bounded_regular_bytes(path, budget=9 * 1024 * 1024)
            if hashlib.sha256(raw).hexdigest() != digest:
                _control_fail("CUTOVER_JOURNAL_CHANGED")
            payload = load_strict_json_text(raw.decode("utf-8"))
            names = ("before_state", "after_state", "before_registration", "after_registration")
            if (
                not isinstance(payload, dict)
                or set(payload) != {"schema_version", *names}
                or payload.get("schema_version") != "workflow_cutover_journal.v1"
            ):
                _control_fail("CUTOVER_JOURNAL_SCHEMA")
            documents: dict[str, bytes] = {}
            decoded: dict[str, dict[str, Any]] = {}
            for name in names:
                value = payload[name]
                if (
                    not isinstance(value, str)
                    or not value
                    or len(value) > 2 * 1024 * 1024
                    or len(value) % 2
                    or re.fullmatch(r"[0-9a-f]+", value) is None
                ):
                    _control_fail("CUTOVER_JOURNAL_DOCUMENT")
                documents[name] = bytes.fromhex(value)
                body = load_strict_json_text(documents[name].decode("utf-8"))
                if not isinstance(body, dict):
                    _control_fail("CUTOVER_JOURNAL_DOCUMENT")
                decoded[name] = body
            for prefix in ("before", "after"):
                _validate_control_state(decoded[prefix + "_state"], root)
                registration = _validate_host_registration(decoded[prefix + "_registration"])
                if registration["control_root"] != root.as_posix():
                    _control_fail("CUTOVER_JOURNAL_ROOT")
            _cutover_publication_position(
                **documents,
                observed_state=documents["before_state"],
                observed_registration=documents["before_registration"],
            )
            yield documents

    @contextmanager
    def hold_confidential_file(self, path: Path) -> Iterator[bytes]:
        """Hold a small secret envelope with exact SY/BA-only owner/group/DACL.

        No creation, ACL repair, decryption, credential logging or account action.
        The returned bytes are ciphertext; plaintext must have a separate lifetime.
        """
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

        if not path.is_absolute() or ".." in path.parts:
            _control_fail("CONFIDENTIAL_FILE_PATH")
        with self._security(confidential=True) as attributes:
            self._assert_exact_security(path, attributes.descriptor)
            # Envelope allocation limit; not a password policy or secret length.
            raw = bounded_regular_bytes(path, budget=16 * 1024)
            with self.hold_protected_files({path: hashlib.sha256(raw).hexdigest()}):
                self._assert_exact_security(path, attributes.descriptor)
                try:
                    yield raw
                finally:
                    self._assert_exact_security(path, attributes.descriptor)

    def create_root(self, root: Path) -> None:
        self.assert_protected(root.parent, parent=True)
        create = self._bind(
            self.kernel, "CreateDirectoryW", [self.w.LPCWSTR, self.c.c_void_p], self.w.BOOL
        )
        with self._security() as attributes:
            if not create(str(root), self.c.byref(attributes)):
                raise self.c.WinError(self.c.get_last_error())
        self.assert_protected(root)

    def _assert_exact_security(self, path: Path, expected: Any) -> None:
        c, w = self.c, self.w
        get = self._bind(
            self.advapi, "GetNamedSecurityInfoW",
            [w.LPWSTR, c.c_int, w.DWORD, c.c_void_p, c.c_void_p,
             c.c_void_p, c.c_void_p, c.POINTER(c.c_void_p)], w.DWORD,
        )
        convert = self._bind(
            self.advapi, "ConvertSecurityDescriptorToStringSecurityDescriptorW",
            [c.c_void_p, w.DWORD, w.DWORD, c.POINTER(w.LPWSTR), c.c_void_p], w.BOOL,
        )

        def text(descriptor: Any) -> str:
            value = w.LPWSTR()
            if not convert(descriptor, 1, 7, c.byref(value), None):
                raise c.WinError(c.get_last_error())
            try:
                return str(value.value)
            finally:
                self.kernel.LocalFree(c.cast(value, c.c_void_p))

        actual = c.c_void_p()
        error = get(str(path), 1, 7, None, None, None, None, c.byref(actual))
        if error:
            raise c.WinError(error)
        try:
            if text(actual) != text(expected):
                _control_fail("EXCHANGE_SECURITY_CHANGED")
        finally:
            self.kernel.LocalFree(actual)

    def create_worker_exchange(self, root: Path, worker_token: Any) -> dict[str, Any]:
        """Create only new scratch objects under a protected parent; retain failures.

        This is an administrative transport, not enrollment or execution authority.
        No request bytes or candidate code are evaluated here.
        """
        from ai_trading_system.platform.architecture.workflow_execution import WindowsWorkerToken

        if type(worker_token) is not WindowsWorkerToken:
            _control_fail("EXCHANGE_LIVE_WORKER_REQUIRED")
        worker = worker_token.validate_launcher()
        if not root.is_absolute() or root.name in {"", ".", ".."}:
            _control_fail("EXCHANGE_ROOT_PATH")
        create_directory = self._bind(
            self.kernel, "CreateDirectoryW", [self.w.LPCWSTR, self.c.c_void_p], self.w.BOOL,
        )
        create_file = self._bind(
            self.kernel, "CreateFileW",
            [self.w.LPCWSTR, self.w.DWORD, self.w.DWORD, self.c.c_void_p,
             self.w.DWORD, self.w.DWORD, self.w.HANDLE], self.w.HANDLE,
        )
        with self.pin_directories((root.parent,)):
            self.assert_protected(root.parent, parent=True)
            with self._security(worker_sid=str(worker["sid"]), exchange_role="root") as attributes:
                if not create_directory(str(root), self.c.byref(attributes)):
                    raise self.c.WinError(self.c.get_last_error())
                self._assert_exact_security(root, attributes.descriptor)
            with self.pin_directories((root,)):
                for role, path in (("result", root / "result.json"),
                                   ("profile", root / "profile")):
                    with self._security(worker_sid=str(worker["sid"]),
                                        exchange_role=role) as attributes:
                        if role == "profile":
                            if not create_directory(str(path), self.c.byref(attributes)):
                                raise self.c.WinError(self.c.get_last_error())
                        else:
                            handle = create_file(str(path), 0x12019F, 0,
                                                 self.c.byref(attributes), 1, 0x80, None)
                            if handle == self.c.c_void_p(-1).value:
                                raise self.c.WinError(self.c.get_last_error())
                            if not self.kernel.CloseHandle(handle):
                                raise self.c.WinError(self.c.get_last_error())
                        self._assert_exact_security(path, attributes.descriptor)
                result_info = (root / "result.json").stat()
                return {
                    "root": directory_identity(root),
                    "profile": directory_identity(root / "profile"),
                    "result_identity": [result_info.st_dev, result_info.st_ino],
                    "worker_identity": worker,
                }

    def publish_root(self, prepared: Path, root: Path) -> None:
        move = self._bind(
            self.kernel, "MoveFileExW", [self.w.LPCWSTR, self.w.LPCWSTR, self.w.DWORD], self.w.BOOL
        )
        if not move(str(prepared), str(root), 0x08):
            raise self.c.WinError(self.c.get_last_error())

    def write_admin_json(self, path: Path, value: Mapping[str, Any]) -> None:
        """Flush a protected staging file, then publish without replacing a file.

        A crash may retain an administrative staging file; it is never evidence
        or a recovery authority. Existing final bytes are immutable.
        """
        import uuid

        c, w = self.c, self.w
        if path.exists():
            self.assert_protected(path)
            if _control_json(path) != value:
                _control_fail("ENROLLMENT_ARTIFACT_CHANGED")
            return
        content = (json.dumps(value, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
        if len(content) > 1024 * 1024:
            _control_fail("ENROLLMENT_ARTIFACT_TOO_LARGE")
        create = self._bind(
            self.kernel,
            "CreateFileW",
            [w.LPCWSTR, w.DWORD, w.DWORD, c.c_void_p, w.DWORD, w.DWORD, w.HANDLE],
            w.HANDLE,
        )
        write = self._bind(
            self.kernel,
            "WriteFile",
            [w.HANDLE, c.c_void_p, w.DWORD, c.POINTER(w.DWORD), c.c_void_p],
            w.BOOL,
        )
        flush = self._bind(self.kernel, "FlushFileBuffers", [w.HANDLE], w.BOOL)
        pending = path.with_name("." + path.name + ".pending-" + uuid.uuid4().hex)
        with self._security() as attributes:
            handle = create(str(pending), 0x40000000, 1, c.byref(attributes), 1, 0x80, None)
        if handle == c.c_void_p(-1).value:
            raise c.WinError(c.get_last_error())
        try:
            written = w.DWORD()
            buffer = c.create_string_buffer(content)
            if not write(handle, buffer, len(content), c.byref(written), None) or (
                written.value != len(content) or not flush(handle)
            ):
                raise c.WinError(c.get_last_error())
        finally:
            self.kernel.CloseHandle(handle)
        move = self._bind(self.kernel, "MoveFileExW", [w.LPCWSTR, w.LPCWSTR, w.DWORD], w.BOOL)
        if not move(str(pending), str(path), 0x08):  # WRITE_THROUGH; no REPLACE_EXISTING.
            raise c.WinError(c.get_last_error())
        self.assert_protected(path)
        if _control_json(path) != value:
            _control_fail("ENROLLMENT_ARTIFACT_CHANGED")

    def _publish_cutover_state(self, custody: _CutoverRecoveryCustody) -> str:
        """Internal transport, not an old-writer fence or activation entry point.

        The caller must establish the OS fence before invoking publication.
        The live journal and original arbiters serialize cooperating publishers;
        administrator protection excludes ordinary writers. This is not an OS
        compare-and-swap against other administrators. Failed staging is retained.
        """
        import uuid

        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

        position = custody.assert_current()
        path = custody.root / CONTROL_STATE_NAME
        self.assert_protected(custody.root)
        self.assert_protected(path)
        if position != "BEFORE_PUBLICATION":
            return position
        content = custody.documents["after_state"]
        c, w = self.c, self.w
        pending = path.with_name("." + path.name + ".pending-" + uuid.uuid4().hex)
        create = self._bind(
            self.kernel, "CreateFileW",
            [w.LPCWSTR, w.DWORD, w.DWORD, c.c_void_p, w.DWORD, w.DWORD, w.HANDLE],
            w.HANDLE,
        )
        write = self._bind(
            self.kernel, "WriteFile",
            [w.HANDLE, c.c_void_p, w.DWORD, c.POINTER(w.DWORD), c.c_void_p], w.BOOL,
        )
        flush = self._bind(self.kernel, "FlushFileBuffers", [w.HANDLE], w.BOOL)
        with self._security() as attributes:
            handle = create(str(pending), 0x40000000, 1, c.byref(attributes), 1, 0x80, None)
        if handle == c.c_void_p(-1).value:
            raise c.WinError(c.get_last_error())
        try:
            written = w.DWORD()
            buffer = c.create_string_buffer(content)
            if not write(handle, buffer, len(content), c.byref(written), None):
                raise c.WinError(c.get_last_error())
            if written.value != len(content):
                _control_fail("CUTOVER_STATE_SHORT_WRITE")
            if not flush(handle):
                raise c.WinError(c.get_last_error())
        finally:
            self.kernel.CloseHandle(handle)
        self.assert_protected(pending)
        if bounded_regular_bytes(pending, budget=1024 * 1024) != content:
            _control_fail("CUTOVER_STATE_STAGING_CHANGED")
        # No retry or repair if anything changed during preparation.
        if custody.assert_current() != "BEFORE_PUBLICATION":
            _control_fail("CUTOVER_STATE_POSITION_CHANGED")
        self.assert_protected(path)
        move = self._bind(self.kernel, "MoveFileExW", [w.LPCWSTR, w.LPCWSTR, w.DWORD], w.BOOL)
        if not move(str(pending), str(path), 0x09):  # REPLACE_EXISTING | WRITE_THROUGH.
            raise c.WinError(c.get_last_error())
        self.assert_protected(path)
        position = custody.assert_current()
        if position != "STATE_PUBLISHED":
            _control_fail("CUTOVER_STATE_PUBLICATION_CHANGED")
        return position

    def _publish_cutover_registration(self, custody: _CutoverRecoveryCustody) -> str:
        """Publish exact journal text after state; never create a missing key.

        Like state transport this requires caller-established OS fencing and
        the original live custody. A flush failure is not retried: subsequent
        recovery must inspect the actual finite publication position anew.
        """
        import winreg

        position = custody.assert_current()
        self.assert_protected("MACHINE\\" + HOST_REGISTRY_KEY, registry=True)
        if position not in {"STATE_PUBLISHED", "REGISTRATION_PUBLISHED"}:
            _control_fail("CUTOVER_REGISTRATION_ORDER")
        before = custody.documents["before_registration"]
        after = custody.documents["after_registration"]
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, HOST_REGISTRY_KEY, 0,
            winreg.KEY_READ | winreg.KEY_SET_VALUE | winreg.KEY_WOW64_64KEY,
        ) as key:
            content, kind = winreg.QueryValueEx(key, HOST_REGISTRY_VALUE)
            if kind != winreg.REG_SZ or not isinstance(content, str):
                _control_fail("CUTOVER_REGISTRATION_CHANGED")
            expected = before if position == "STATE_PUBLISHED" else after
            if content.encode("utf-8") != expected:
                _control_fail("CUTOVER_REGISTRATION_CHANGED")
            if custody.assert_current() != position:
                _control_fail("CUTOVER_REGISTRATION_CHANGED")
            if position == "STATE_PUBLISHED":
                winreg.SetValueEx(key, HOST_REGISTRY_VALUE, 0, winreg.REG_SZ, after.decode("utf-8"))
            # Already-visible bytes can follow a failed FlushKey. Recovery must
            # still flush them, without issuing a duplicate value write.
            winreg.FlushKey(key)
            content, kind = winreg.QueryValueEx(key, HOST_REGISTRY_VALUE)
            if kind != winreg.REG_SZ or not isinstance(content, str):
                _control_fail("CUTOVER_REGISTRATION_CHANGED")
            if content.encode("utf-8") != after:
                _control_fail("CUTOVER_REGISTRATION_CHANGED")
        self.assert_protected("MACHINE\\" + HOST_REGISTRY_KEY, registry=True)
        position = custody.assert_current()
        if position != "REGISTRATION_PUBLISHED":
            _control_fail("CUTOVER_REGISTRATION_CHANGED")
        return position

    @contextmanager
    def registry_key(self, name: str) -> Iterator[int]:
        import winreg

        c, w = self.c, self.w
        create = self._bind(
            self.advapi,
            "RegCreateKeyExW",
            [
                w.HANDLE,
                w.LPCWSTR,
                w.DWORD,
                w.LPWSTR,
                w.DWORD,
                w.DWORD,
                c.c_void_p,
                c.POINTER(w.HANDLE),
                c.POINTER(w.DWORD),
            ],
            w.LONG,
        )
        close = self._bind(self.advapi, "RegCloseKey", [w.HANDLE], w.LONG)
        handle, disposition = w.HANDLE(), w.DWORD()
        with self._security(registry=True) as attributes:
            machine = c.c_void_p(c.c_int32(int(winreg.HKEY_LOCAL_MACHINE)).value).value
            code = create(
                machine,
                name,
                0,
                None,
                0,
                winreg.KEY_READ | winreg.KEY_WRITE | winreg.KEY_WOW64_64KEY,
                c.byref(attributes),
                c.byref(handle),
                c.byref(disposition),
            )
        if code:
            raise c.WinError(code)
        try:
            self.assert_protected("MACHINE\\" + name, registry=True)
            yield int(handle.value)
        finally:
            close(handle)

    def registry_payload(self, handle: int) -> dict[str, Any] | None:
        import winreg

        from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

        # The protected parent holds exactly the one anchor value: any subkey
        # (including the legacy WorkflowControl key) or foreign value is unknown.
        subkeys, values, _modified = winreg.QueryInfoKey(handle)
        if subkeys or values > 1:
            _control_fail("HOST_REGISTRATION_ANCHOR_CHANGED")
        if not values:
            return None
        name, content, kind = winreg.EnumValue(handle, 0)
        if name != HOST_REGISTRY_VALUE or kind != winreg.REG_SZ or len(content) > 1024 * 1024:
            _control_fail("HOST_REGISTRATION_ANCHOR_CHANGED")
        return _validate_host_registration(load_strict_json_text(content))

    def write_registry_payload(self, handle: int, registration: Mapping[str, Any]) -> None:
        import winreg

        winreg.SetValueEx(
            handle, HOST_REGISTRY_VALUE, 0, winreg.REG_SZ, json.dumps(registration, sort_keys=True)
        )
        winreg.FlushKey(handle)

    def register(self, registration: Mapping[str, Any]) -> None:
        """Write the one anchor value; idempotent, never overwrites different text.

        One REG_SZ value write is atomic, so an absent value means not enrolled
        and a present value is complete. Check-then-write is not atomic across
        processes (RegRenameKey's destination-exists refusal was); the admin-only
        protected key, arbiter-serialized enrollment and read-back comparison
        bound that residual race, which fails closed rather than overwriting.
        """
        existing = _trusted_host_registration()
        if existing is not None:
            if existing != registration:
                _control_fail("HOST_REGISTRATION_CHANGED")
            self.assert_protected("MACHINE\\" + HOST_REGISTRY_KEY, registry=True)
            return
        self.assert_protected(r"MACHINE\SOFTWARE", registry=True, allow_inheritance=True)
        with self.registry_key(HOST_REGISTRY_KEY) as handle:
            _reject_legacy_host_registry_key()
            payload = self.registry_payload(handle)
            if payload is None:
                self.write_registry_payload(handle, registration)
            elif payload != registration:
                _control_fail("HOST_REGISTRATION_CHANGED")
            if self.registry_payload(handle) != registration:
                _control_fail("HOST_REGISTRATION_CHANGED")
        self.assert_protected("MACHINE\\" + HOST_REGISTRY_KEY, registry=True)
        if _trusted_host_registration() != registration:
            _control_fail("HOST_REGISTRATION_CHANGED")


def _enrollment_documents(
    plan: Mapping[str, Any],
    root: Path,
    *,
    markers: Mapping[str, str],
    prepared_root: Path | None = None,
) -> dict:
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest

    identity = {**directory_identity(prepared_root or root), "path": root.as_posix()}
    shared = {
        "host_id": plan["host_id"],
        "epoch": "enrollment-" + plan["plan_sha256"],
        "root_identity": identity,
        "resource_markers": dict(markers),
        "policy_sha256": plan["policy_sha256"],
    }
    registrations, locators, machine_repositories = [], [], []
    grouped: dict[str, list[Any]] = {}
    for row in plan["repositories"]:
        grouped.setdefault(row["common_identity"]["path"], []).append(row)
    for common, rows in sorted(grouped.items()):
        registration = {
            "registration_id": canonical_digest(rows[0]["common_identity"]),
            "common_root": common,
            "common_identity": rows[0]["common_identity"],
            "entrypoints": ["checkout-guard"],
        }
        registrations.append(registration)
        locator = {
            "schema_version": "workflow_host_binding.v1",
            **shared,
            "control_root": root.as_posix(),
            **{key: value for key, value in registration.items() if key != "entrypoints"},
        }
        content = (json.dumps(locator, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
        locators.append(
            {"path": (Path(common) / CONTROL_BINDING_NAME).as_posix(), "value": locator}
        )
        machine_repositories.append(
            {
                "common_identity": rows[0]["common_identity"],
                "checkout_identities": [row["checkout_identity"] for row in rows],
                "locator_sha256": hashlib.sha256(content).hexdigest(),
            }
        )
    state = {
        "schema_version": "workflow_host_control.v1",
        "phase": "DRAINING",
        **shared,
        FULL_EXECUTION_POLICY_KEY: "PROTECTED_WORKER_REQUIRED",
        "registrations": registrations,
        "legacy_roots": [
            {"root_identity": row["root_identity"], "epoch": shared["epoch"]}
            for row in plan["legacy_stores"]
        ],
    }
    state_bytes = (json.dumps(state, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    return {
        "state": state,
        "locators": locators,
        "retirements": [
            {
                "path": (Path(row["root_identity"]["path"]) / RETIREMENT_NAME).as_posix(),
                "value": {
                    "schema_version": "workflow_legacy_retirement.v1",
                    "root_identity": row["root_identity"],
                    "control_root": root.as_posix(),
                    "epoch": shared["epoch"],
                },
            }
            for row in plan["legacy_stores"]
        ],
        "registration": {
            "schema_version": "workflow_machine_registration.v1",
            "host_id": plan["host_id"],
            "control_root": root.as_posix(),
            "root_identity": identity,
            "repositories": machine_repositories,
            "state_sha256": hashlib.sha256(state_bytes).hexdigest(),
        },
    }


def _finish_host_enrollment(
    journal: Mapping[str, Any],
    *,
    policy: Any,
    actor: str,
    administrator: _WindowsEnrollmentAdministrator,
) -> dict[str, Any]:
    """Resume the same desired DRAINING state under the original store arbiters."""
    from ai_trading_system.platform.architecture.lease_arbiter import hold_lease_arbiter
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        FileExecutionLeaseStore,
    )
    from ai_trading_system.platform.architecture.workflow_contract import (
        canonical_digest,
        portable_path,
        repository_identity,
    )

    plan, documents = journal["plan"], journal["documents"]
    root = Path(plan["target"]["path"])
    if plan["host_id"] != machine_host_id() or plan["plan_sha256"] != canonical_digest(
        {key: value for key, value in plan.items() if key != "plan_sha256"}
    ):
        _control_fail("ENROLLMENT_ARTIFACT_CHANGED")
    for marker in journal["resource_markers"].values():
        portable_path(marker)
    if (
        control_policy_sha256(policy) != plan["policy_sha256"]
        or actor not in policy.allowlisted_actors
    ):
        _control_fail("CUTOVER_ACTOR_OR_POLICY")
    if documents != _enrollment_documents(plan, root, markers=journal["resource_markers"]):
        _control_fail("ENROLLMENT_ARTIFACT_CHANGED")
    existing = _trusted_host_registration()
    if existing is not None and existing != documents["registration"]:
        _control_fail("HOST_REGISTRATION_CHANGED")
    identities = [
        documents["state"]["root_identity"],
        *(row["root_identity"] for row in plan["legacy_stores"]),
    ]
    identities.sort(key=lambda row: (row["device"], row["file_id"]))
    with ExitStack() as stack:
        stack.enter_context(
            administrator.pin_directories(
                [
                    *(Path(row["path"]) for row in identities),
                    *(Path(row["common_identity"]["path"]) for row in plan["repositories"]),
                    *(Path(row["checkout_identity"]["path"]) for row in plan["repositories"]),
                ]
            )
        )
        administrator.assert_protected(root)
        for row in plan["repositories"]:
            checkout = Path(row["repository"]["checkout"])
            if (
                repository_identity(checkout) != row["repository"]
                or (directory_identity(checkout) != row["checkout_identity"])
                or directory_identity(Path(row["repository"]["common"])) != row["common_identity"]
            ):
                _control_fail("REPOSITORY_IDENTITY_CHANGED")
        for identity in identities:
            current = Path(identity["path"])
            if directory_identity(current) != identity:
                _control_fail("CUTOVER_ROOT_CHANGED")
            stack.enter_context(
                hold_lease_arbiter(
                    current,
                    actor=actor,
                    now=datetime.now(UTC),
                    arbiter_ttl_seconds=policy.arbiter_ttl_seconds,
                )
            )
            replay = FileExecutionLeaseStore(current, policy=policy).replay()
            if replay.status != "PASS" or (current == root and replay.event_count):
                _control_fail("CUTOVER_REPLAY_INVALID")
        # ACTIVE legacy heads remain in place and may drain through old roots.
        # No event/lease copying or TTL-based release occurs here.
        administrator.write_admin_json(root / CONTROL_STATE_NAME, documents["state"])
        for item in [*documents["locators"], *documents["retirements"]]:
            administrator.write_admin_json(Path(item["path"]), item["value"])
        administrator.register(documents["registration"])
        for row in plan["repositories"]:
            binding = resolve_host_control_binding(
                Path(row["repository"]["checkout"]), entrypoint="checkout-guard"
            )
            if (
                binding is None
                or binding.assert_current(operation="observe")["phase"] != "DRAINING"
            ):
                _control_fail("ENROLLMENT_READBACK_FAILED")
    return {
        "schema_version": "workflow_host_enrollment_result.v1",
        "status": "DRAINING",
        "plan_sha256": plan["plan_sha256"],
        "control_root": root.as_posix(),
        "activation_allowed": False,
        "old_binary_os_fence_installed": False,
        "mutation_performed": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _publish_enrollment_preparation(
    journal: Mapping[str, Any],
    root: Path,
    *,
    policy: Any,
    administrator: _WindowsEnrollmentAdministrator,
) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import (
        canonical_digest,
        portable_path,
        repository_identity,
    )

    plan = journal["plan"]
    prepared = _enrollment_preparation_path(root)
    if journal.get("schema_version") != "workflow_host_enrollment_journal.v1" or (
        plan["host_id"] != machine_host_id()
        or plan["policy_sha256"] != control_policy_sha256(policy)
        or plan["plan_sha256"]
        != canonical_digest({key: value for key, value in plan.items() if key != "plan_sha256"})
        or plan["target"]["path"] != root.as_posix()
        or plan["target"]["preparation_path"] != prepared.as_posix()
        or directory_identity(root.parent) != plan["target"]["parent_identity"]
        or set(journal["resource_markers"]) != {"full", "publication"}
    ):
        _control_fail("ENROLLMENT_ARTIFACT_CHANGED")
    for marker in journal["resource_markers"].values():
        portable_path(marker)
    for row in plan["repositories"]:
        checkout = Path(row["repository"]["checkout"])
        if (
            repository_identity(checkout) != row["repository"]
            or (directory_identity(checkout) != row["checkout_identity"])
            or directory_identity(Path(row["repository"]["common"])) != row["common_identity"]
        ):
            _control_fail("REPOSITORY_IDENTITY_CHANGED")
    for row in plan["legacy_stores"]:
        if directory_identity(Path(row["root_identity"]["path"])) != row["root_identity"]:
            _control_fail("CUTOVER_ROOT_CHANGED")
    with administrator.pin_directories([root.parent]):
        administrator.assert_protected(root.parent, parent=True)
        location = root if root.exists() else prepared
        administrator.assert_protected(location)
        administrator.assert_protected(location / ENROLLMENT_JOURNAL_NAME)
        if _control_json(location / ENROLLMENT_JOURNAL_NAME) != journal or (
            journal["documents"]
            != _enrollment_documents(
                plan, root, markers=journal["resource_markers"], prepared_root=location
            )
        ):
            _control_fail("ENROLLMENT_ARTIFACT_CHANGED")
        if location == prepared:
            try:
                administrator.publish_root(prepared, root)
            except OSError:
                # A concurrent equal intent may have published this exact inode.
                # Anything else retains its original failure; no replacement.
                if not root.exists():
                    raise
        administrator.assert_protected(root)
        administrator.assert_protected(root / ENROLLMENT_JOURNAL_NAME)
        if directory_identity(root) != journal["documents"]["state"]["root_identity"] or (
            _control_json(root / ENROLLMENT_JOURNAL_NAME) != journal
        ):
            _control_fail("ENROLLMENT_ARTIFACT_CHANGED")


def enroll_host_draining(
    project_root: Path,
    *,
    policy: Any,
    actor: str,
    plan_path: Path | None = None,
    recover_root: Path | None = None,
) -> dict[str, Any]:
    """Explicit administrator entry: first enrollment/recovery, never activation."""
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        load_publication_fence_policy,
    )
    from ai_trading_system.platform.architecture.workflow_contract import repository_identity

    administrator = _WindowsEnrollmentAdministrator()  # Before any requested-path read/write.
    if (plan_path is None) == (recover_root is None):
        _control_fail("ENROLLMENT_INPUT")
    if actor not in policy.allowlisted_actors:
        _control_fail("CUTOVER_ACTOR")
    if recover_root is not None:
        root = recover_root.absolute()
        location = root if root.exists() else _enrollment_preparation_path(root)
        administrator.assert_protected(location)
        if not (location / ENROLLMENT_JOURNAL_NAME).exists():
            _control_fail("ENROLLMENT_PREPARE_INCOMPLETE")
        administrator.assert_protected(location / ENROLLMENT_JOURNAL_NAME)
        journal = _control_json(location / ENROLLMENT_JOURNAL_NAME)
        if journal.get("schema_version") != "workflow_host_enrollment_journal.v1" or (
            journal["plan"]["target"]["path"] != root.as_posix()
        ):
            _control_fail("ENROLLMENT_ARTIFACT_CHANGED")
        if repository_identity(project_root) not in [
            row["repository"] for row in journal["plan"]["repositories"]
        ]:
            _control_fail("HOST_REPOSITORY_NOT_REGISTERED")
        _publish_enrollment_preparation(journal, root, policy=policy, administrator=administrator)
        return _finish_host_enrollment(
            journal, policy=policy, actor=actor, administrator=administrator
        )
    plan = _control_json(plan_path)
    if not plan.get("repositories"):
        _control_fail("ENROLLMENT_INPUT")
    current = plan_host_enrollment(
        project_root,
        policy=policy,
        control_root=Path(plan["target"]["path"]),
        legacy_roots=[Path(row["root_identity"]["path"]) for row in plan["legacy_stores"]],
        additional_repositories=[
            Path(row["repository"]["checkout"]) for row in plan["repositories"][1:]
        ],
    )
    if current != plan or plan["target"]["existing_identity"] is not None:
        _control_fail("ENROLLMENT_PLAN_STALE")
    fence_policy = load_publication_fence_policy(
        project_root / "config/architecture/arch_005_integration_publication_fence.yaml"
    )
    markers = {
        "full": fence_policy.exclusive_validation_resource,
        "publication": fence_policy.exclusive_publication_resource,
    }
    root = Path(plan["target"]["path"])
    prepared = _enrollment_preparation_path(root)
    with administrator.pin_directories([root.parent]):
        if not prepared.exists():
            administrator.create_root(prepared)
        administrator.assert_protected(prepared)
        if not (prepared / ENROLLMENT_JOURNAL_NAME).exists():
            for child in prepared.iterdir():
                if not re.fullmatch(
                    r"\.host-enrollment\.v1\.json\.pending-[0-9a-f]{32}", child.name
                ):
                    _control_fail("ENROLLMENT_PREPARATION_NOT_EMPTY")
                administrator.assert_protected(child)
        journal = {
            "schema_version": "workflow_host_enrollment_journal.v1",
            "plan": plan,
            "resource_markers": markers,
            "documents": _enrollment_documents(plan, root, markers=markers, prepared_root=prepared),
        }
        administrator.write_admin_json(prepared / ENROLLMENT_JOURNAL_NAME, journal)
        _publish_enrollment_preparation(journal, root, policy=policy, administrator=administrator)
        return _finish_host_enrollment(
            journal, policy=policy, actor=actor, administrator=administrator
        )


def inspect_control_drain(project_root: Path, *, policy: Any) -> dict[str, Any]:
    """Observe old lease ledgers without expiring, releasing or activating them.

    This is a diagnostic snapshot, not a cutover capability. An empty ledger
    cannot prove the absence of an old binary or an unrecorded live child.
    The eventual administrator transition must repeat its complete checks while
    holding the existing old/new arbiters in a fixed order.
    """
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        FileExecutionLeaseStore,
    )
    from ai_trading_system.platform.architecture.workflow_contract import repository_identity

    identity = repository_identity(project_root)
    binding = resolve_host_control_binding(project_root, entrypoint="checkout-guard")
    if binding is None:
        return {
            "schema_version": "workflow_drain_inspection.v1",
            "status": "NOT_ENROLLED",
            "repository": identity,
            "lease_drain_state": "UNAVAILABLE",
            "roots": [],
            "activation_allowed": False,
            "mutation_performed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    state = binding.assert_current(operation="observe")
    if control_policy_sha256(policy) != binding.policy_sha256:
        _control_fail("STORE_POLICY_CHANGED")
    roots: list[dict[str, Any]] = []
    for registered in sorted(state["legacy_roots"], key=lambda row: row["root_identity"]["path"]):
        root = Path(registered["root_identity"]["path"])
        if directory_identity(root) != registered["root_identity"]:
            _control_fail("LEGACY_ROOT_IDENTITY_CHANGED")
        marker = _control_json(root / RETIREMENT_NAME)
        if marker != {
            "schema_version": "workflow_legacy_retirement.v1",
            "root_identity": registered["root_identity"],
            "control_root": binding.root.as_posix(),
            "epoch": state["epoch"],
        }:
            _control_fail("RETIREMENT_NOT_REGISTERED")
        replay = FileExecutionLeaseStore(root, policy=policy).replay()
        # Expired wall time is deliberately irrelevant: it grants no ownership.
        active = sorted(head.lease_id for head in replay.lease_heads if head.state == "ACTIVE")
        unresolved = sorted(
            head.lease_id
            for head in replay.lease_heads
            if not execution_is_terminal(head.execution)
        )
        roots.append(
            {
                "root_identity": registered["root_identity"],
                "replay_status": replay.status,
                "event_count": replay.event_count,
                "head_event_ids": dict(replay.head_event_ids),
                "active_lease_ids": active,
                "unresolved_execution_lease_ids": unresolved,
                "issue_codes": sorted({issue.code for issue in replay.issues}),
            }
        )
        if directory_identity(root) != registered["root_identity"]:
            _control_fail("LEGACY_ROOT_IDENTITY_CHANGED")
    if binding.assert_current(operation="observe") != state:
        _control_fail("DRAIN_OBSERVATION_CHANGED")
    blocked = any(
        row["replay_status"] != "PASS"
        or row["active_lease_ids"]
        or row["unresolved_execution_lease_ids"]
        for row in roots
    )
    return {
        "schema_version": "workflow_drain_inspection.v1",
        "status": "DRAIN_REQUIRED" if blocked else "OS_FENCE_EVIDENCE_REQUIRED",
        "repository": identity,
        "epoch": binding.epoch,
        "phase": state["phase"],
        "lease_drain_state": "BLOCKED" if blocked else "CLEAR_SNAPSHOT_ONLY",
        "roots": roots,
        "remaining_checks": [
            "OLD_ENTRYPOINT_OS_FENCE",
            "REAL_EXECUTOR_INVENTORY",
            "ATOMIC_CUTOVER",
        ],
        "activation_allowed": False,
        "mutation_performed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
