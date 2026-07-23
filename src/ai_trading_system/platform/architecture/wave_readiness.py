from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
import tarfile
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from ai_trading_system.platform.architecture.devex import (
    build_aggregate_shadow_index,
    build_module_manifest,
    build_test_manifest,
)
from ai_trading_system.platform.architecture.parallel_control import (
    LaneRole,
    ParallelControlError,
    build_deterministic_lane_plan,
    parse_change_manifest,
)
from ai_trading_system.platform.architecture.task_registry_shadow import (
    ACTIVE_REGISTER_PATH,
    COMPLETED_REGISTER_PATH,
    SHADOW_FRAGMENT_SCHEMA_VERSION,
    SHADOW_REGISTRY_ROOT,
    build_s0_baseline,
    build_shadow_fragment,
    build_shadow_index,
    load_legacy_documents,
    shadow_fragment_path,
    validate_s0_baseline,
    validate_shadow_fragment,
    validate_shadow_index,
)

POLICY_SCHEMA_VERSION = "architecture_wave_readiness_policy.v1"
EVIDENCE_SCHEMA_VERSION = "architecture_wave_parallel_readiness.v1"
PRODUCER_SCHEMA_VERSION = "architecture_wave_readiness_producer.v1"

GENERATOR_MODULE_PATH = "src/ai_trading_system/platform/architecture/wave_readiness.py"
GENERATOR_CLI_PATH = "scripts/architecture_wave_readiness.py"
_REPLAY_DEPENDENCY_PATHS = frozenset(
    {
        GENERATOR_MODULE_PATH,
        GENERATOR_CLI_PATH,
        "src/ai_trading_system/platform/architecture/devex.py",
        "src/ai_trading_system/platform/architecture/parallel_control.py",
        "src/ai_trading_system/platform/architecture/task_registry_shadow.py",
        "src/ai_trading_system/yaml_loader.py",
    }
)

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_REF_RE = re.compile(r"^(?:HEAD|FETCH_HEAD|[A-Za-z0-9][A-Za-z0-9._/-]{0,254})$")
_EXPONENT_NUMBER_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)[eE][+-]?\d+$")
_WINDOWS_RESERVED_NAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }
)
_ALLOWED_TAR_MEMBER_TYPES = frozenset({tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE})
_SNAPSHOT_SCAN_ROOTS = ("scripts", "src/ai_trading_system", "tests")

_POLICY_KEYS = frozenset(
    {
        "schema_version",
        "wave",
        "gate",
        "status",
        "owner",
        "source_wave",
        "lane_base",
        "max_parallel_domain_lanes",
        "selected_domains",
        "source_bindings",
        "task_bindings",
        "generated_state",
        "change_manifests",
        "coordinator_only_paths",
        "worktree_guard",
        "lease_authority",
        "assignment_control",
        "safety",
    }
)
_SOURCE_WAVE_KEYS = frozenset({"wave_id", "commit", "tree_sha1"})
_LANE_BASE_KEYS = frozenset({"commit", "tree_sha1", "branch", "remote_ref"})
_SELECTED_DOMAIN_KEYS = frozenset({"domain_id", "change_id", "task_id"})
_SOURCE_BINDING_KEYS = frozenset(
    {
        "binding_id",
        "binding_kind",
        "commit_role",
        "path",
        "blob_sha256",
        "schema_version",
        "status",
    }
)
_TASK_BINDING_KEYS = frozenset(
    {
        "task_id",
        "source_register_path",
        "source_register_blob_sha256",
        "row_sha256",
        "priority",
        "status",
        "shadow_fragment_path",
        "shadow_fragment_blob_sha256",
        "shadow_fragment_schema_version",
        "shadow_fragment_checksum",
        "requirement_paths",
    }
)
_GENERATED_STATE_KEYS = frozenset(
    {
        "ownership_policy_path",
        "module_manifest_path",
        "test_manifest_path",
        "aggregate_index_path",
        "task_baseline_path",
        "task_shadow_index_path",
        "task_fragment_root",
    }
)
_WORKTREE_GUARD_KEYS = frozenset({"known_unrelated_paths", "pending_output_paths"})
_LEASE_AUTHORITY_KEYS = frozenset(
    {
        "kind",
        "lease_namespace_created",
        "lease_acquisition_allowed",
        "active_shared_path_lease_count",
    }
)
_ASSIGNMENT_CONTROL_KEYS = frozenset(
    {
        "mode",
        "authority",
        "worker_assignment_allowed_after_s0_pass",
        "carrier_commit_push_required",
        "automatic_command_dispatch",
        "automatic_merge",
    }
)
_SAFETY_KEYS = frozenset(
    {
        "dispatch_allowed",
        "automatic_command_dispatch",
        "lease_acquisition_allowed",
        "automatic_merge_allowed",
        "consumer_cutover_allowed",
        "task_registry_mutation_allowed",
        "production_effect",
        "broker_action",
    }
)

_EVIDENCE_KEYS = frozenset(
    {
        "schema_version",
        "wave",
        "gate",
        "status",
        "owner",
        "policy_binding",
        "producer",
        "source_wave",
        "lane_base",
        "source_bindings",
        "task_bindings",
        "generated_state_replay",
        "change_manifests",
        "lane_plan",
        "worktree_guard",
        "lease_authority",
        "assignment_control",
        "safety",
        "evidence_checksum",
    }
)


class WaveReadinessError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: Any,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise WaveReadinessError("YAML_NON_STRING_KEY", f"line={key_node.start_mark.line + 1}")
        if key in result:
            raise WaveReadinessError("YAML_DUPLICATE_KEY", key)
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_strict_yaml_text(text: str, *, label: str = "yaml") -> object:
    try:
        value = yaml.load(text, Loader=_UniqueKeySafeLoader)
    except WaveReadinessError:
        raise
    except RecursionError as exc:
        raise WaveReadinessError("YAML_CYCLIC_ALIAS", label) from exc
    except yaml.YAMLError as exc:
        if "recursive" in str(exc).lower():
            raise WaveReadinessError("YAML_CYCLIC_ALIAS", label) from exc
        raise WaveReadinessError("YAML_INVALID", label) from exc
    _reject_non_finite(value, label)
    return value


def load_strict_yaml_path(path: Path) -> object:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise WaveReadinessError("YAML_READ", str(path)) from exc
    return load_strict_yaml_text(text, label=str(path))


def load_strict_json_text(text: str, *, label: str = "json") -> object:
    def reject_constant(value: str) -> object:
        raise WaveReadinessError("JSON_NON_FINITE", f"{label}:{value}")

    try:
        value = json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=reject_constant,
        )
    except WaveReadinessError:
        raise
    except json.JSONDecodeError as exc:
        raise WaveReadinessError("JSON_INVALID", label) from exc
    _reject_non_finite(value, label)
    return value


def load_strict_json_path(path: Path) -> object:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise WaveReadinessError("JSON_READ", str(path)) from exc
    return load_strict_json_text(text, label=str(path))


def load_wave_readiness_policy(path: Path) -> dict[str, Any]:
    payload = _mapping(load_strict_yaml_path(path), "policy")
    policy = dict(payload)
    validate_wave_readiness_policy(policy)
    return policy


def load_wave_readiness_evidence(path: Path) -> dict[str, Any]:
    payload = _mapping(load_strict_json_path(path), "evidence")
    evidence = dict(payload)
    _validate_evidence_structure(evidence)
    return evidence


def validate_wave_readiness_policy(payload: Mapping[str, Any]) -> None:
    _reject_non_finite(payload, "policy")
    _exact_keys(payload, _POLICY_KEYS, "POLICY_FIELDS")
    if payload["schema_version"] != POLICY_SCHEMA_VERSION:
        raise WaveReadinessError("POLICY_SCHEMA", f"expected={POLICY_SCHEMA_VERSION}")
    _identifier(payload["wave"], "wave")
    _identifier(payload["gate"], "gate")
    if payload["status"] != "SCOPE_FROZEN_NOT_DISPATCHED":
        raise WaveReadinessError(
            "POLICY_STATUS",
            "status must equal SCOPE_FROZEN_NOT_DISPATCHED",
        )
    owner = _text(payload["owner"], "owner")

    source_wave = _mapping(payload["source_wave"], "source_wave")
    _exact_keys(source_wave, _SOURCE_WAVE_KEYS, "SOURCE_WAVE_FIELDS")
    _identifier(source_wave["wave_id"], "source_wave.wave_id")
    _commit(source_wave["commit"], "source_wave.commit")
    _commit(source_wave["tree_sha1"], "source_wave.tree_sha1")

    lane_base = _mapping(payload["lane_base"], "lane_base")
    _exact_keys(lane_base, _LANE_BASE_KEYS, "LANE_BASE_FIELDS")
    lane_base_commit = _commit(lane_base["commit"], "lane_base.commit")
    if source_wave["commit"] == lane_base_commit:
        raise WaveReadinessError(
            "SOURCE_LANE_COMMIT_IDENTITY", "source wave B and lane base C must differ"
        )
    _commit(lane_base["tree_sha1"], "lane_base.tree_sha1")
    _git_ref(lane_base["branch"], "lane_base.branch")
    _remote_ref(lane_base["remote_ref"], "lane_base.remote_ref")

    maximum = _integer(
        payload["max_parallel_domain_lanes"],
        "max_parallel_domain_lanes",
        minimum=1,
    )
    if maximum != 2:
        raise WaveReadinessError("DOMAIN_LANE_CAPACITY", "max_parallel_domain_lanes must equal 2")

    selected = _records(payload["selected_domains"], "selected_domains")
    if len(selected) != 2:
        raise WaveReadinessError("SELECTED_DOMAIN_COUNT", f"expected=2 actual={len(selected)}")
    selected_ids: set[str] = set()
    selected_change_ids: set[str] = set()
    selected_task_ids: set[str] = set()
    selected_change_tasks: dict[str, str] = {}
    for index, row in enumerate(selected):
        _exact_keys(row, _SELECTED_DOMAIN_KEYS, "SELECTED_DOMAIN_FIELDS")
        domain_id = _identifier(row["domain_id"], f"selected_domains[{index}].domain_id")
        change_id = _identifier(row["change_id"], f"selected_domains[{index}].change_id")
        task_id = _identifier(row["task_id"], f"selected_domains[{index}].task_id")
        if domain_id in selected_ids or change_id in selected_change_ids:
            raise WaveReadinessError("DUPLICATE_SELECTED_DOMAIN", domain_id)
        selected_ids.add(domain_id)
        selected_change_ids.add(change_id)
        selected_task_ids.add(task_id)
        selected_change_tasks[change_id] = task_id
    _require_canonical_record_order(selected, "domain_id", "selected_domains")

    source_bindings = _validate_source_bindings(payload["source_bindings"])
    if not any(row["commit_role"] == "SOURCE_WAVE" for row in source_bindings):
        raise WaveReadinessError(
            "SOURCE_WAVE_BINDING_MISSING",
            "at least one source binding must use SOURCE_WAVE",
        )

    generated = _mapping(payload["generated_state"], "generated_state")
    _exact_keys(generated, _GENERATED_STATE_KEYS, "GENERATED_STATE_FIELDS")
    generated_paths = {
        field: _portable_path(generated[field], f"generated_state.{field}")
        for field in sorted(_GENERATED_STATE_KEYS)
    }
    if generated_paths["task_fragment_root"] != SHADOW_REGISTRY_ROOT:
        raise WaveReadinessError(
            "TASK_FRAGMENT_ROOT",
            f"expected={SHADOW_REGISTRY_ROOT}",
        )
    generated_output_paths = {
        generated_paths[field]
        for field in (
            "module_manifest_path",
            "test_manifest_path",
            "aggregate_index_path",
            "task_baseline_path",
            "task_shadow_index_path",
        )
    }
    generated_bound_paths = {
        str(row["path"])
        for row in source_bindings
        if row["binding_kind"] == "GENERATED_STATE" and row["commit_role"] == "LANE_BASE"
    }
    if generated_bound_paths != generated_output_paths:
        raise WaveReadinessError(
            "GENERATED_SOURCE_BINDING_SET",
            (
                f"missing={sorted(generated_output_paths - generated_bound_paths)} "
                f"unknown={sorted(generated_bound_paths - generated_output_paths)}"
            ),
        )

    coordinator_paths = _canonical_paths(
        payload["coordinator_only_paths"], "coordinator_only_paths"
    )
    if not coordinator_paths:
        raise WaveReadinessError(
            "COORDINATOR_PATHS_EMPTY", "coordinator_only_paths cannot be empty"
        )

    raw_manifests = _records(payload["change_manifests"], "change_manifests")
    if len(raw_manifests) != 3:
        raise WaveReadinessError("CHANGE_MANIFEST_COUNT", f"expected=3 actual={len(raw_manifests)}")
    manifests = []
    for raw in raw_manifests:
        try:
            manifests.append(parse_change_manifest(raw))
        except ParallelControlError as exc:
            raise WaveReadinessError(f"MANIFEST_{exc.code}", exc.message) from exc
    if len({manifest.change_id for manifest in manifests}) != len(manifests):
        raise WaveReadinessError("DUPLICATE_CHANGE_ID", "change_id must be unique")
    _require_canonical_manifest_order(raw_manifests)
    if any(manifest.base_commit != lane_base_commit for manifest in manifests):
        raise WaveReadinessError("MANIFEST_BASE_DRIFT", "all change manifests must use lane base C")
    domains = [manifest for manifest in manifests if manifest.lane_role is LaneRole.DOMAIN]
    coordinators = [
        manifest for manifest in manifests if manifest.lane_role is LaneRole.COORDINATOR
    ]
    if len(domains) != 2 or len(coordinators) != 1:
        raise WaveReadinessError(
            "LANE_ROLE_CARDINALITY",
            f"domains={len(domains)} coordinators={len(coordinators)}",
        )
    if selected_change_ids != {manifest.change_id for manifest in domains}:
        raise WaveReadinessError(
            "SELECTED_CHANGE_BINDING", "selected domains must bind both domain manifests"
        )
    if selected_task_ids != {manifest.task_id for manifest in domains}:
        raise WaveReadinessError(
            "SELECTED_TASK_BINDING", "selected domains must bind both domain task ids"
        )
    if selected_change_tasks != {manifest.change_id: manifest.task_id for manifest in domains}:
        raise WaveReadinessError(
            "SELECTED_CHANGE_TASK_PAIR",
            "each selected change_id and task_id must bind the same domain manifest",
        )
    coordinator = coordinators[0]
    if tuple(coordinator.shared_paths) != coordinator_paths:
        raise WaveReadinessError(
            "COORDINATOR_SHARED_PATH_SET",
            "coordinator shared_paths must equal coordinator_only_paths",
        )
    lane_plan = build_deterministic_lane_plan(
        manifests,
        current_base_commit=lane_base_commit,
        coordinator_only_paths=list(coordinator_paths),
        max_parallel_domain_lanes=maximum,
    )
    if lane_plan.status != "PASS":
        codes = sorted(issue.code for issue in lane_plan.blocking_issues)
        raise WaveReadinessError("LANE_PLAN_BLOCKED", ",".join(codes))
    wave_kinds = [str(wave.get("kind")) for wave in lane_plan.waves]
    if wave_kinds != ["DOMAIN", "COORDINATOR"]:
        raise WaveReadinessError(
            "LANE_PLAN_SHAPE", f"expected DOMAIN->COORDINATOR actual={wave_kinds}"
        )
    first_assignments = lane_plan.waves[0].get("assignments")
    if not isinstance(first_assignments, list) or len(first_assignments) != 2:
        raise WaveReadinessError(
            "DOMAIN_PLAN_WIDTH", "both domain changes must occupy one two-lane wave"
        )

    task_bindings = _validate_task_bindings(payload["task_bindings"])
    task_ids = {str(row["task_id"]) for row in task_bindings}
    manifest_task_ids = {manifest.task_id for manifest in manifests}
    if task_ids != manifest_task_ids:
        raise WaveReadinessError(
            "TASK_BINDING_SET",
            (
                f"missing={sorted(manifest_task_ids - task_ids)} "
                f"unknown={sorted(task_ids - manifest_task_ids)}"
            ),
        )
    requirement_bound_paths = {
        str(row["path"])
        for row in source_bindings
        if row["binding_kind"] == "REQUIREMENT" and row["commit_role"] == "LANE_BASE"
    }
    required_paths = {
        str(path)
        for row in task_bindings
        for path in _sequence(row["requirement_paths"], "requirement_paths")
    }
    if required_paths != requirement_bound_paths:
        raise WaveReadinessError(
            "REQUIREMENT_SOURCE_BINDING_SET",
            (
                f"missing={sorted(required_paths - requirement_bound_paths)} "
                f"unknown={sorted(requirement_bound_paths - required_paths)}"
            ),
        )

    guard = _mapping(payload["worktree_guard"], "worktree_guard")
    unrelated, pending = _validate_worktree_allowlist_shapes(guard)
    protected_files = {
        GENERATOR_MODULE_PATH,
        GENERATOR_CLI_PATH,
        *generated_paths.values(),
        *(str(row["path"]) for row in source_bindings),
        *(str(row["source_register_path"]) for row in task_bindings),
        *(str(row["shadow_fragment_path"]) for row in task_bindings),
        *(
            str(path)
            for row in task_bindings
            for path in _sequence(row["requirement_paths"], "requirement_paths")
        ),
        *(path for manifest in manifests for path in manifest.owned_paths),
        *(path for manifest in manifests for path in manifest.shared_paths),
        *coordinator_paths,
    }
    protected_roots = {generated_paths["task_fragment_root"]}
    collisions = sorted(
        path
        for path in unrelated | pending
        if path in protected_files
        or any(path == root or path.startswith(f"{root}/") for root in protected_roots)
    )
    if collisions:
        raise WaveReadinessError("WORKTREE_ALLOWLIST_PROTECTED_PATH", ",".join(collisions))

    _validate_lease_authority(payload["lease_authority"])
    _validate_assignment_control(payload["assignment_control"], owner=owner)
    _validate_safety(payload["safety"])


def build_wave_readiness_evidence(
    *,
    project_root: Path,
    policy_path: Path,
    output_path: Path | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    policy_file = policy_path.resolve()
    policy = load_wave_readiness_policy(policy_file)
    policy_portable = _relative_to_root(policy_file, root)
    pending = set(
        _canonical_paths(
            _mapping(policy["worktree_guard"], "worktree_guard")["pending_output_paths"],
            "worktree_guard.pending_output_paths",
        )
    )
    if not _is_readiness_policy_output(policy_portable):
        raise WaveReadinessError("CARRIER_POLICY_PATH_FAMILY", policy_portable)
    if policy_portable not in pending:
        raise WaveReadinessError("POLICY_NOT_DECLARED_PENDING", policy_portable)
    if output_path is not None:
        output_portable = _relative_to_root(output_path.resolve(), root)
        if not _is_readiness_evidence_output(output_portable):
            raise WaveReadinessError("CARRIER_EVIDENCE_PATH_FAMILY", output_portable)
        if {policy_portable, output_portable} != pending:
            raise WaveReadinessError(
                "CARRIER_OUTPUT_PATH_SET",
                (
                    f"declared={sorted(pending)} "
                    f"actual={sorted({policy_portable, output_portable})}"
                ),
            )
    return _materialize_evidence(
        project_root=root,
        policy_path=policy_file,
        policy=policy,
        build_mode=True,
    )


def validate_wave_readiness_evidence(
    payload: Mapping[str, Any],
    *,
    project_root: Path,
    policy_path: Path,
    evidence_path: Path,
) -> None:
    _validate_evidence_structure(payload)
    root = project_root.resolve()
    policy_file = policy_path.resolve()
    evidence_file = evidence_path.resolve()
    policy = load_wave_readiness_policy(policy_file)
    _validate_carrier_state(
        project_root=root,
        policy=policy,
        policy_path=policy_file,
        evidence_path=evidence_file,
        evidence=payload,
    )
    expected = _materialize_evidence(
        project_root=root,
        policy_path=policy_file,
        policy=policy,
        build_mode=False,
    )
    if dict(payload) != expected:
        raise WaveReadinessError(
            "EVIDENCE_REPLAY_DRIFT",
            _first_difference(dict(payload), expected),
        )


def canonical_evidence_bytes(payload: Mapping[str, Any]) -> bytes:
    _validate_evidence_structure(payload)
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def calculate_evidence_checksum(payload: Mapping[str, Any]) -> str:
    return _canonical_sha256(
        {key: value for key, value in payload.items() if key != "evidence_checksum"}
    )


def git_resolve_ref(project_root: Path, ref: str) -> str:
    validated = _git_ref(ref, "ref")
    result = _git_process(
        project_root.resolve(),
        "rev-parse",
        "--verify",
        f"{validated}^{{commit}}",
    )
    if result.returncode != 0:
        raise WaveReadinessError("GIT_REF_UNAVAILABLE", f"{validated}:{_git_error(result)}")
    return _commit(result.stdout.decode("ascii").strip(), f"ref:{validated}")


def _git_resolve_remote_tracking_ref(project_root: Path, ref: str) -> str:
    validated = _remote_ref(ref, "remote_ref")
    symbolic = _git_process(
        project_root.resolve(),
        "rev-parse",
        "--symbolic-full-name",
        "--verify",
        validated,
    )
    if symbolic.returncode != 0:
        raise WaveReadinessError(
            "GIT_REMOTE_REF_UNAVAILABLE",
            f"{validated}:{_git_error(symbolic)}",
        )
    full_name = symbolic.stdout.decode("utf-8", errors="strict").strip()
    if (
        not full_name.startswith("refs/remotes/")
        or "\n" in full_name
        or full_name.endswith("/HEAD")
    ):
        raise WaveReadinessError(
            "GIT_REMOTE_TRACKING_REF_REQUIRED",
            f"{validated}:{full_name}",
        )
    return git_resolve_ref(project_root, full_name)


def git_commit_exists(project_root: Path, commit: str) -> bool:
    validated = _commit(commit, "commit")
    result = _git_process(project_root.resolve(), "cat-file", "-e", f"{validated}^{{commit}}")
    if result.returncode in {0, 1, 128}:
        return result.returncode == 0
    raise WaveReadinessError("GIT_COMMIT_CHECK", _git_error(result))


def git_commit_tree(project_root: Path, commit: str) -> str:
    validated = _commit(commit, "commit")
    result = _git_process(project_root.resolve(), "rev-parse", "--verify", f"{validated}^{{tree}}")
    if result.returncode != 0:
        raise WaveReadinessError("GIT_TREE_UNAVAILABLE", f"{validated}:{_git_error(result)}")
    return _commit(result.stdout.decode("ascii").strip(), "tree")


def git_blob_bytes(project_root: Path, commit: str, path: str) -> bytes:
    validated_commit = _commit(commit, "commit")
    portable = _portable_path(path, "git_blob.path")
    result = _git_process(
        project_root.resolve(),
        "cat-file",
        "blob",
        f"{validated_commit}:{portable}",
    )
    if result.returncode != 0:
        raise WaveReadinessError(
            "GIT_BLOB_UNAVAILABLE",
            f"{validated_commit}:{portable}:{_git_error(result)}",
        )
    return result.stdout


def git_blob_sha256(project_root: Path, commit: str, path: str) -> str:
    return hashlib.sha256(git_blob_bytes(project_root, commit, path)).hexdigest()


def git_is_ancestor(project_root: Path, ancestor_commit: str, descendant_commit: str) -> bool:
    ancestor = _commit(ancestor_commit, "ancestor_commit")
    descendant = _commit(descendant_commit, "descendant_commit")
    result = _git_process(
        project_root.resolve(),
        "merge-base",
        "--is-ancestor",
        ancestor,
        descendant,
    )
    if result.returncode not in {0, 1}:
        raise WaveReadinessError("GIT_ANCESTRY_CHECK", _git_error(result))
    return result.returncode == 0


def get_worktree_dirty_paths(
    project_root: Path,
    *,
    excluded_paths: Sequence[str] = (),
) -> tuple[str, ...]:
    excluded = tuple(
        sorted({_portable_path(path, "worktree.excluded_paths") for path in excluded_paths})
    )
    pathspec = (
        ("--", ".", *(f":(top,literal,exclude){path}" for path in excluded)) if excluded else ()
    )
    result = _git_process(
        project_root.resolve(),
        "status",
        "--porcelain=v2",
        "-z",
        "--untracked-files=all",
        *pathspec,
    )
    if result.returncode != 0:
        raise WaveReadinessError("GIT_STATUS", _git_error(result))
    records = result.stdout.split(b"\0")
    paths: set[str] = set()
    skip_rename_source = False
    for raw in records:
        if not raw:
            continue
        if skip_rename_source:
            paths.add(_decode_status_path(raw))
            skip_rename_source = False
            continue
        prefix = raw[:2]
        if prefix in {b"? ", b"! "}:
            paths.add(_decode_status_path(raw[2:]))
        elif prefix in {b"1 ", b"u "}:
            fields = raw.split(b" ", 8 if prefix == b"1 " else 10)
            if len(fields) < (9 if prefix == b"1 " else 11):
                raise WaveReadinessError("GIT_STATUS_PARSE", raw.decode("utf-8", errors="replace"))
            paths.add(_decode_status_path(fields[-1]))
        elif prefix == b"2 ":
            fields = raw.split(b" ", 9)
            if len(fields) < 10:
                raise WaveReadinessError("GIT_STATUS_PARSE", raw.decode("utf-8", errors="replace"))
            paths.add(_decode_status_path(fields[-1]))
            skip_rename_source = True
        else:
            raise WaveReadinessError("GIT_STATUS_PARSE", raw.decode("utf-8", errors="replace"))
    if skip_rename_source:
        raise WaveReadinessError("GIT_STATUS_PARSE", "rename source path missing")
    return tuple(sorted(paths))


def assert_worktree_guard(
    *,
    project_root: Path,
    guard: Mapping[str, Any],
) -> dict[str, Any]:
    known, pending = _validate_worktree_allowlist_shapes(guard)
    dirty = set(
        get_worktree_dirty_paths(
            project_root,
            excluded_paths=tuple(sorted(known)),
        )
    )
    unexpected = sorted(dirty - known - pending)
    if unexpected:
        raise WaveReadinessError("WORKTREE_UNEXPECTED_PATHS", ",".join(unexpected))
    return {
        "status": "PASS",
        "known_unrelated_paths": sorted(known),
        "pending_output_paths": sorted(pending),
        "unexpected_paths": [],
        "known_unrelated_path_bytes_read": False,
        "pending_output_presence_recorded": False,
    }


def _validate_worktree_allowlist_shapes(
    guard: Mapping[str, Any],
) -> tuple[set[str], set[str]]:
    _exact_keys(guard, _WORKTREE_GUARD_KEYS, "WORKTREE_GUARD_FIELDS")
    known = set(
        _canonical_paths(
            guard["known_unrelated_paths"],
            "worktree_guard.known_unrelated_paths",
        )
    )
    pending = set(
        _canonical_paths(
            guard["pending_output_paths"],
            "worktree_guard.pending_output_paths",
        )
    )
    if known & pending:
        raise WaveReadinessError("WORKTREE_ALLOWLIST_OVERLAP", str(sorted(known & pending)))
    invalid_known = sorted(
        path for path in known if not path.startswith("docs/research/") or not path.endswith(".md")
    )
    if invalid_known:
        raise WaveReadinessError("WORKTREE_KNOWN_UNRELATED_FAMILY", ",".join(invalid_known))
    policy_outputs = [path for path in pending if _is_readiness_policy_output(path)]
    evidence_outputs = [path for path in pending if _is_readiness_evidence_output(path)]
    if (
        len(pending) != 2
        or len(policy_outputs) != 1
        or len(evidence_outputs) != 1
        or set(policy_outputs + evidence_outputs) != pending
    ):
        raise WaveReadinessError(
            "WORKTREE_PENDING_OUTPUT_FAMILY",
            "pending outputs must be one architecture readiness YAML and one readiness JSON",
        )
    return known, pending


def _is_readiness_policy_output(path: str) -> bool:
    return (
        path.startswith("config/architecture/")
        and path.endswith((".yaml", ".yml"))
        and "readiness" in PurePosixPath(path).stem.lower()
    )


def _is_readiness_evidence_output(path: str) -> bool:
    return (
        path.startswith("inputs/architecture/")
        and path.endswith(".json")
        and "readiness" in PurePosixPath(path).stem.lower()
    )


def _validate_carrier_state(
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    policy_path: Path,
    evidence_path: Path,
    evidence: Mapping[str, Any],
) -> None:
    lane_base = _mapping(policy["lane_base"], "lane_base")
    lane_commit = _commit(lane_base["commit"], "lane_base.commit")
    remote_ref = _remote_ref(lane_base["remote_ref"], "lane_base.remote_ref")
    head = _validate_carrier_commit(
        project_root=project_root,
        lane_commit=lane_commit,
        remote_ref=remote_ref,
    )
    policy_portable = _relative_to_root(policy_path, project_root)
    evidence_portable = _relative_to_root(evidence_path, project_root)
    if not _is_readiness_policy_output(policy_portable):
        raise WaveReadinessError("CARRIER_POLICY_PATH_FAMILY", policy_portable)
    if not _is_readiness_evidence_output(evidence_portable):
        raise WaveReadinessError("CARRIER_EVIDENCE_PATH_FAMILY", evidence_portable)
    policy_binding = _mapping(evidence["policy_binding"], "policy_binding")
    if policy_binding["path"] != policy_portable:
        raise WaveReadinessError(
            "CARRIER_POLICY_PATH_BINDING",
            f"expected={policy_portable} actual={policy_binding['path']}",
        )
    pending = set(
        _canonical_paths(
            _mapping(policy["worktree_guard"], "worktree_guard")["pending_output_paths"],
            "worktree_guard.pending_output_paths",
        )
    )
    if {policy_portable, evidence_portable} != pending:
        raise WaveReadinessError(
            "CARRIER_ARTIFACT_PATH_SET",
            (
                f"expected={sorted(pending)} "
                f"actual={sorted({policy_portable, evidence_portable})}"
            ),
        )
    carrier = _locate_carrier_commit(
        project_root=project_root,
        lane_commit=lane_commit,
        head=head,
    )
    _validate_carrier_diff_scope(
        project_root=project_root,
        lane_commit=lane_commit,
        head=carrier,
        allowed_paths={policy_portable, evidence_portable},
    )
    _validate_carrier_dependency_blobs(
        project_root=project_root,
        lane_commit=lane_commit,
        head=carrier,
    )
    _assert_carrier_blob_matches(
        project_root=project_root,
        head=carrier,
        path=policy_portable,
        local_bytes=policy_path.read_bytes(),
        label="POLICY",
    )
    evidence_bytes = evidence_path.read_bytes()
    if evidence_bytes != canonical_evidence_bytes(evidence):
        raise WaveReadinessError("CARRIER_EVIDENCE_CANONICAL_BYTES", evidence_portable)
    _assert_carrier_blob_matches(
        project_root=project_root,
        head=carrier,
        path=evidence_portable,
        local_bytes=evidence_bytes,
        label="EVIDENCE",
    )


def _validate_carrier_commit(
    *,
    project_root: Path,
    lane_commit: str,
    remote_ref: str,
) -> str:
    validated_lane_commit = _commit(lane_commit, "lane_commit")
    validated_remote_ref = _remote_ref(remote_ref, "remote_ref")
    head = git_resolve_ref(project_root, "HEAD")
    remote = _git_resolve_remote_tracking_ref(project_root, validated_remote_ref)
    if head == validated_lane_commit or not git_is_ancestor(
        project_root, validated_lane_commit, head
    ):
        raise WaveReadinessError(
            "CARRIER_COMMIT_REQUIRED",
            f"C={validated_lane_commit} HEAD={head}",
        )
    if (
        remote == validated_lane_commit
        or not git_is_ancestor(project_root, validated_lane_commit, remote)
        or not git_is_ancestor(project_root, remote, head)
    ):
        raise WaveReadinessError(
            "CARRIER_PUSH_DRIFT",
            f"HEAD={head} {validated_remote_ref}={remote}",
        )
    return head


def _locate_carrier_commit(
    *,
    project_root: Path,
    lane_commit: str,
    head: str,
) -> str:
    validated_lane = _commit(lane_commit, "lane_commit")
    validated_head = _commit(head, "head")
    result = _git_process(
        project_root.resolve(),
        "rev-list",
        "--first-parent",
        "--reverse",
        f"{validated_lane}..{validated_head}",
    )
    if result.returncode != 0:
        raise WaveReadinessError("CARRIER_LINEAGE_UNAVAILABLE", _git_error(result))
    commits = [
        _commit(row, "carrier_lineage.commit")
        for row in result.stdout.decode("ascii").splitlines()
        if row
    ]
    if not commits:
        raise WaveReadinessError(
            "CARRIER_COMMIT_REQUIRED",
            f"C={validated_lane} HEAD={validated_head}",
        )
    carrier = commits[0]
    parent_result = _git_process(
        project_root.resolve(),
        "rev-parse",
        "--verify",
        f"{carrier}^1",
    )
    if parent_result.returncode != 0:
        raise WaveReadinessError("CARRIER_PARENT_UNAVAILABLE", _git_error(parent_result))
    parent = _commit(parent_result.stdout.decode("ascii").strip(), "carrier.parent")
    if parent != validated_lane:
        raise WaveReadinessError(
            "CARRIER_DIRECT_CHILD_REQUIRED",
            f"C={validated_lane} D={carrier} D^={parent}",
        )
    return carrier


def _validate_carrier_diff_scope(
    *,
    project_root: Path,
    lane_commit: str,
    head: str,
    allowed_paths: set[str],
) -> None:
    validated_lane = _commit(lane_commit, "lane_commit")
    validated_head = _commit(head, "head")
    expected = {_portable_path(path, "carrier.allowed_path") for path in allowed_paths}
    result = _git_process(
        project_root.resolve(),
        "diff",
        "--name-only",
        "-z",
        "--no-renames",
        validated_lane,
        validated_head,
        "--",
    )
    if result.returncode != 0:
        raise WaveReadinessError("CARRIER_DIFF_UNAVAILABLE", _git_error(result))
    actual = {_decode_status_path(raw) for raw in result.stdout.split(b"\0") if raw}
    if actual != expected:
        raise WaveReadinessError(
            "CARRIER_DIFF_SCOPE",
            (f"missing={sorted(expected - actual)} " f"unexpected={sorted(actual - expected)}"),
        )


def _validate_carrier_dependency_blobs(
    *,
    project_root: Path,
    lane_commit: str,
    head: str,
    dependency_paths: Sequence[str] | None = None,
) -> None:
    validated_lane = _commit(lane_commit, "lane_commit")
    validated_head = _commit(head, "head")
    paths = (
        tuple(sorted(_REPLAY_DEPENDENCY_PATHS))
        if dependency_paths is None
        else tuple(
            sorted({_portable_path(path, "carrier.dependency_path") for path in dependency_paths})
        )
    )
    drifted = [
        path
        for path in paths
        if git_blob_bytes(project_root, validated_lane, path)
        != git_blob_bytes(project_root, validated_head, path)
    ]
    if drifted:
        raise WaveReadinessError("CARRIER_REPLAY_DEPENDENCY_DRIFT", ",".join(drifted))


def _assert_carrier_blob_matches(
    *,
    project_root: Path,
    head: str,
    path: str,
    local_bytes: bytes,
    label: str,
) -> None:
    try:
        tracked_bytes = git_blob_bytes(project_root, head, path)
    except WaveReadinessError as exc:
        if exc.code != "GIT_BLOB_UNAVAILABLE":
            raise
        raise WaveReadinessError(f"CARRIER_{label}_UNTRACKED", f"{head}:{path}") from exc
    if tracked_bytes != local_bytes:
        raise WaveReadinessError(f"CARRIER_{label}_BLOB_DRIFT", f"{head}:{path}")


def _materialize_evidence(
    *,
    project_root: Path,
    policy_path: Path,
    policy: Mapping[str, Any],
    build_mode: bool,
) -> dict[str, Any]:
    validate_wave_readiness_policy(policy)
    source_wave = _mapping(policy["source_wave"], "source_wave")
    lane_base = _mapping(policy["lane_base"], "lane_base")
    source_commit = _commit(source_wave["commit"], "source_wave.commit")
    lane_commit = _commit(lane_base["commit"], "lane_base.commit")
    if source_commit == lane_commit:
        raise WaveReadinessError(
            "SOURCE_LANE_COMMIT_IDENTITY", "source wave B and lane base C must differ"
        )
    for label, commit in (("B", source_commit), ("C", lane_commit)):
        if not git_commit_exists(project_root, commit):
            raise WaveReadinessError("GIT_COMMIT_UNKNOWN", f"{label}={commit}")
    source_tree = git_commit_tree(project_root, source_commit)
    lane_tree = git_commit_tree(project_root, lane_commit)
    if source_tree != source_wave["tree_sha1"]:
        raise WaveReadinessError(
            "SOURCE_WAVE_TREE_DRIFT",
            f"expected={source_wave['tree_sha1']} actual={source_tree}",
        )
    if lane_tree != lane_base["tree_sha1"]:
        raise WaveReadinessError(
            "LANE_BASE_TREE_DRIFT",
            f"expected={lane_base['tree_sha1']} actual={lane_tree}",
        )
    if not git_is_ancestor(project_root, source_commit, lane_commit):
        raise WaveReadinessError("SOURCE_LANE_ANCESTRY", f"B={source_commit} C={lane_commit}")

    head = git_resolve_ref(project_root, "HEAD")
    remote_ref = _remote_ref(lane_base["remote_ref"], "lane_base.remote_ref")
    remote = _git_resolve_remote_tracking_ref(project_root, remote_ref)
    branch = _current_branch(project_root)
    expected_branch = _text(lane_base["branch"], "lane_base.branch")
    if branch != expected_branch:
        raise WaveReadinessError(
            "LANE_BRANCH_DRIFT",
            f"expected={expected_branch} actual={branch}",
        )
    if build_mode:
        fetch_head = git_resolve_ref(project_root, "FETCH_HEAD")
        if head != lane_commit or fetch_head != lane_commit or remote != lane_commit:
            raise WaveReadinessError(
                "LANE_BASE_BUILD_REF_DRIFT",
                (f"C={lane_commit} HEAD={head} FETCH_HEAD={fetch_head} " f"{remote_ref}={remote}"),
            )
    else:
        _validate_carrier_commit(
            project_root=project_root,
            lane_commit=lane_commit,
            remote_ref=remote_ref,
        )

    guard_result = assert_worktree_guard(
        project_root=project_root,
        guard=_mapping(policy["worktree_guard"], "worktree_guard"),
    )
    source_binding_rows = _verify_source_bindings(
        project_root=project_root,
        policy=policy,
    )
    snapshot_archive_paths = _snapshot_archive_paths(
        project_root=project_root,
        lane_commit=lane_commit,
        policy=policy,
    )
    known_unrelated_paths = tuple(str(path) for path in guard_result["known_unrelated_paths"])
    _validate_snapshot_path_separation(
        archive_paths=snapshot_archive_paths,
        excluded_paths=known_unrelated_paths,
    )
    with _git_commit_snapshot(
        project_root,
        lane_commit,
        archive_paths=snapshot_archive_paths,
        forbidden_paths=known_unrelated_paths,
    ) as snapshot_root:
        replay = _replay_generated_state(
            project_root=project_root,
            snapshot_root=snapshot_root,
            lane_commit=lane_commit,
            policy=policy,
            snapshot_archive_paths=snapshot_archive_paths,
            snapshot_excluded_paths=known_unrelated_paths,
        )
        task_binding_rows = _verify_task_bindings(
            project_root=project_root,
            snapshot_root=snapshot_root,
            lane_commit=lane_commit,
            policy=policy,
        )

    raw_manifests = _records(policy["change_manifests"], "change_manifests")
    manifests = [parse_change_manifest(row) for row in raw_manifests]
    lane_plan = build_deterministic_lane_plan(
        manifests,
        current_base_commit=lane_commit,
        coordinator_only_paths=list(
            _canonical_paths(policy["coordinator_only_paths"], "coordinator_only_paths")
        ),
        max_parallel_domain_lanes=2,
    )
    if lane_plan.status != "PASS":
        raise WaveReadinessError("LANE_PLAN_BLOCKED", str(lane_plan.to_dict()))

    policy_portable = _relative_to_root(policy_path, project_root)
    policy_sha256 = _normalized_text_sha256(policy_path)
    producer = {
        "schema_version": PRODUCER_SCHEMA_VERSION,
        "module_path": GENERATOR_MODULE_PATH,
        "module_blob_sha256": git_blob_sha256(project_root, lane_commit, GENERATOR_MODULE_PATH),
        "cli_path": GENERATOR_CLI_PATH,
        "cli_blob_sha256": git_blob_sha256(project_root, lane_commit, GENERATOR_CLI_PATH),
    }
    evidence: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "wave": policy["wave"],
        "gate": policy["gate"],
        "status": "PASS",
        "owner": policy["owner"],
        "policy_binding": {
            "path": policy_portable,
            "sha256": policy_sha256,
        },
        "producer": producer,
        "source_wave": {
            "wave_id": source_wave["wave_id"],
            "commit": source_commit,
            "tree_sha1": source_tree,
        },
        "lane_base": {
            "commit": lane_commit,
            "tree_sha1": lane_tree,
            "branch": expected_branch,
            "remote_ref": remote_ref,
            "build_ref_state": {
                "HEAD": lane_commit,
                "FETCH_HEAD": lane_commit,
                "remote": lane_commit,
            },
            "source_wave_is_ancestor": True,
            "carrier_commit_recorded": False,
        },
        "source_bindings": source_binding_rows,
        "task_bindings": task_binding_rows,
        "generated_state_replay": replay,
        "change_manifests": [manifest.to_dict() for manifest in manifests],
        "lane_plan": lane_plan.to_dict(),
        "worktree_guard": guard_result,
        "lease_authority": dict(_mapping(policy["lease_authority"], "lease_authority")),
        "assignment_control": dict(_mapping(policy["assignment_control"], "assignment_control")),
        "safety": dict(_mapping(policy["safety"], "safety")),
    }
    evidence["evidence_checksum"] = calculate_evidence_checksum(evidence)
    _validate_evidence_structure(evidence)
    return evidence


def _verify_source_bindings(
    *,
    project_root: Path,
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_wave = _mapping(policy["source_wave"], "source_wave")
    lane_base = _mapping(policy["lane_base"], "lane_base")
    commits = {
        "SOURCE_WAVE": _commit(source_wave["commit"], "source_wave.commit"),
        "LANE_BASE": _commit(lane_base["commit"], "lane_base.commit"),
    }
    verified: list[dict[str, Any]] = []
    for row in _validate_source_bindings(policy["source_bindings"]):
        commit_role = str(row["commit_role"])
        path = str(row["path"])
        raw = git_blob_bytes(project_root, commits[commit_role], path)
        actual_sha = hashlib.sha256(raw).hexdigest()
        if actual_sha != row["blob_sha256"]:
            raise WaveReadinessError(
                "SOURCE_BINDING_HASH",
                f"{row['binding_id']}:{path}:expected={row['blob_sha256']} actual={actual_sha}",
            )
        expected_schema = row["schema_version"]
        expected_status = row["status"]
        if expected_schema is not None or expected_status is not None:
            mapped = _load_mapping_bytes(raw, path)
            if expected_schema is not None and mapped.get("schema_version") != expected_schema:
                raise WaveReadinessError("SOURCE_BINDING_SCHEMA", str(row["binding_id"]))
            if expected_status is not None and mapped.get("status") != expected_status:
                raise WaveReadinessError("SOURCE_BINDING_STATUS", str(row["binding_id"]))
        verified.append(dict(row))
    return verified


def _verify_task_bindings(
    *,
    project_root: Path,
    snapshot_root: Path,
    lane_commit: str,
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    documents = load_legacy_documents(snapshot_root)
    rows = {row.task_id: row for document in documents for row in document.rows}
    verified: list[dict[str, Any]] = []
    for binding in _validate_task_bindings(policy["task_bindings"]):
        task_id = str(binding["task_id"])
        try:
            row = rows[task_id]
        except KeyError as exc:
            raise WaveReadinessError("TASK_BINDING_MISSING", task_id) from exc
        if row.source_path != binding["source_register_path"]:
            raise WaveReadinessError("TASK_REGISTER_PATH", task_id)
        register_sha = git_blob_sha256(project_root, lane_commit, row.source_path)
        if register_sha != binding["source_register_blob_sha256"]:
            raise WaveReadinessError("TASK_REGISTER_HASH", task_id)
        if row.row_sha256 != binding["row_sha256"]:
            raise WaveReadinessError("TASK_ROW_HASH", task_id)
        if (
            row.projected_cells[2] != binding["priority"]
            or row.projected_cells[3] != binding["status"]
        ):
            raise WaveReadinessError("TASK_STATUS_PRIORITY", task_id)

        fragment_path = str(binding["shadow_fragment_path"])
        fragment_file = _snapshot_file(snapshot_root, fragment_path)
        fragment = _mapping(
            load_strict_yaml_path(fragment_file),
            f"task fragment {task_id}",
        )
        validate_shadow_fragment(fragment)
        if shadow_fragment_path(fragment) != fragment_path:
            raise WaveReadinessError("TASK_FRAGMENT_PATH", task_id)
        expected_fragment = build_shadow_fragment(
            row,
            source_commit=str(_mapping(fragment["initial_event"], "initial_event")["base_commit"]),
        )
        if dict(fragment) != expected_fragment:
            raise WaveReadinessError("TASK_FRAGMENT_REPLAY", task_id)
        if (
            hashlib.sha256(fragment_file.read_bytes()).hexdigest()
            != binding["shadow_fragment_blob_sha256"]
            or fragment.get("schema_version") != binding["shadow_fragment_schema_version"]
            or fragment.get("fragment_checksum") != binding["shadow_fragment_checksum"]
        ):
            raise WaveReadinessError("TASK_FRAGMENT_BINDING", task_id)
        task_record = _mapping(fragment["task_record"], "task_record")
        requirement_refs = tuple(
            sorted(
                _portable_path(path, f"{task_id}.requirement_ref")
                for path in _sequence(task_record["requirement_refs"], "requirement_refs")
            )
        )
        requirement_paths = tuple(
            str(path) for path in _sequence(binding["requirement_paths"], "requirement_paths")
        )
        if requirement_paths != requirement_refs:
            raise WaveReadinessError("TASK_REQUIREMENT_BINDING", task_id)
        verified.append(dict(binding))
    return verified


def _replay_generated_state(
    *,
    project_root: Path,
    snapshot_root: Path,
    lane_commit: str,
    policy: Mapping[str, Any],
    snapshot_archive_paths: Sequence[str],
    snapshot_excluded_paths: Sequence[str],
) -> dict[str, Any]:
    generated = _mapping(policy["generated_state"], "generated_state")
    paths = {
        field: _portable_path(generated[field], f"generated_state.{field}")
        for field in _GENERATED_STATE_KEYS
    }
    ownership_path = _snapshot_file(snapshot_root, paths["ownership_policy_path"])
    load_strict_yaml_path(ownership_path)
    fragments_root = _snapshot_dir(snapshot_root, paths["task_fragment_root"])
    for fragment_file in sorted(fragments_root.rglob("*.yaml")):
        load_strict_yaml_path(fragment_file)

    expected_module = build_module_manifest(
        project_root=snapshot_root,
        policy_path=ownership_path,
    )
    expected_test = build_test_manifest(
        project_root=snapshot_root,
        policy_path=ownership_path,
    )
    expected_aggregate = build_aggregate_shadow_index(
        project_root=snapshot_root,
        policy_path=ownership_path,
    )
    artifact_rows = [
        _assert_generated_payload(
            project_root=project_root,
            snapshot_root=snapshot_root,
            lane_commit=lane_commit,
            path=paths["module_manifest_path"],
            expected=expected_module,
        ),
        _assert_generated_payload(
            project_root=project_root,
            snapshot_root=snapshot_root,
            lane_commit=lane_commit,
            path=paths["test_manifest_path"],
            expected=expected_test,
        ),
        _assert_generated_payload(
            project_root=project_root,
            snapshot_root=snapshot_root,
            lane_commit=lane_commit,
            path=paths["aggregate_index_path"],
            expected=expected_aggregate,
        ),
    ]

    documents = load_legacy_documents(snapshot_root)
    baseline_file = _snapshot_file(snapshot_root, paths["task_baseline_path"])
    baseline = _mapping(load_strict_yaml_path(baseline_file), "task registry baseline")
    validate_s0_baseline(baseline, documents=documents)
    entry = _mapping(baseline["entry_gate"], "entry_gate")
    expected_baseline = build_s0_baseline(
        project_root=snapshot_root,
        handoff={
            "schema_version": entry["schema_version"],
            "handoff_checksum": entry["handoff_checksum"],
            "head_commit": entry["source_commit"],
            "next_slice_unblocked": entry["next_slice_unblocked"],
        },
        documents=documents,
    )
    artifact_rows.append(
        _assert_generated_payload(
            project_root=project_root,
            snapshot_root=snapshot_root,
            lane_commit=lane_commit,
            path=paths["task_baseline_path"],
            expected=expected_baseline,
        )
    )

    fragments: list[dict[str, Any]] = []
    fragment_files: list[dict[str, Any]] = []
    source_commit = str(entry["source_commit"])
    ordered_rows = sorted(
        (row for document in documents for row in document.rows),
        key=lambda item: item.task_id,
    )
    expected_fragment_paths = {
        shadow_fragment_path(build_shadow_fragment(row, source_commit=source_commit))
        for row in ordered_rows
    }
    _validate_task_fragment_path_set(
        snapshot_root=snapshot_root,
        fragment_root=fragments_root,
        expected_paths=expected_fragment_paths,
    )
    for row in ordered_rows:
        expected_fragment = build_shadow_fragment(row, source_commit=source_commit)
        path = shadow_fragment_path(expected_fragment)
        fragment_file = _snapshot_file(snapshot_root, path)
        actual_fragment = _mapping(load_strict_yaml_path(fragment_file), f"fragment:{row.task_id}")
        if dict(actual_fragment) != expected_fragment:
            raise WaveReadinessError("TASK_FRAGMENT_REPLAY", f"{row.task_id}:{path}")
        raw = fragment_file.read_bytes()
        git_raw = git_blob_bytes(project_root, lane_commit, path)
        if raw != git_raw:
            raise WaveReadinessError("SNAPSHOT_BLOB_DRIFT", path)
        fragments.append(dict(actual_fragment))
        fragment_files.append(
            {
                "task_id": row.task_id,
                "path": path,
                "file_sha256": hashlib.sha256(raw).hexdigest(),
                "fragment_checksum": actual_fragment["fragment_checksum"],
            }
        )
    expected_index = build_shadow_index(
        baseline=baseline,
        documents=documents,
        fragments=fragments,
        fragment_files=fragment_files,
    )
    index_file = _snapshot_file(snapshot_root, paths["task_shadow_index_path"])
    actual_index = _mapping(load_strict_yaml_path(index_file), "task shadow index")
    validate_shadow_index(
        actual_index,
        baseline=baseline,
        documents=documents,
    )
    if dict(actual_index) != expected_index:
        raise WaveReadinessError("GENERATED_PAYLOAD_DRIFT", paths["task_shadow_index_path"])
    artifact_rows.append(
        _assert_generated_payload(
            project_root=project_root,
            snapshot_root=snapshot_root,
            lane_commit=lane_commit,
            path=paths["task_shadow_index_path"],
            expected=expected_index,
        )
    )
    fragment_set_sha = _canonical_sha256(fragment_files)
    return {
        "status": "PASS",
        "snapshot_commit": lane_commit,
        "snapshot_tree_sha1": git_commit_tree(project_root, lane_commit),
        "snapshot_source": "GIT_ARCHIVE_C_ALLOWLIST_ONLY",
        "snapshot_archive_paths": list(snapshot_archive_paths),
        "snapshot_excluded_paths": list(snapshot_excluded_paths),
        "tracked_payload_equality": True,
        "module_manifest_rebuilt": True,
        "test_manifest_rebuilt": True,
        "aggregate_index_rebuilt": True,
        "task_baseline_rebuilt": True,
        "task_shadow_index_rebuilt": True,
        "task_fragment_count": len(fragment_files),
        "task_fragment_set_sha256": fragment_set_sha,
        "artifacts": sorted(artifact_rows, key=lambda row: str(row["path"])),
        "main_worktree_business_bytes_read": False,
    }


def _validate_task_fragment_path_set(
    *,
    snapshot_root: Path,
    fragment_root: Path,
    expected_paths: set[str],
) -> None:
    actual_paths = {
        _relative_to_root(path, snapshot_root)
        for path in fragment_root.rglob("*.yaml")
        if path.is_file()
    }
    if actual_paths != expected_paths:
        raise WaveReadinessError(
            "TASK_FRAGMENT_PATH_SET",
            (
                f"missing={sorted(expected_paths - actual_paths)} "
                f"orphan={sorted(actual_paths - expected_paths)}"
            ),
        )


def _assert_generated_payload(
    *,
    project_root: Path,
    snapshot_root: Path,
    lane_commit: str,
    path: str,
    expected: Mapping[str, Any],
) -> dict[str, Any]:
    file_path = _snapshot_file(snapshot_root, path)
    raw = file_path.read_bytes()
    git_raw = git_blob_bytes(project_root, lane_commit, path)
    if raw != git_raw:
        raise WaveReadinessError("SNAPSHOT_BLOB_DRIFT", path)
    actual = _mapping(load_strict_yaml_path(file_path), path)
    if dict(actual) != dict(expected):
        raise WaveReadinessError("GENERATED_PAYLOAD_DRIFT", path)
    payload_sha = _canonical_sha256(dict(actual))
    return {
        "path": path,
        "blob_sha256": hashlib.sha256(raw).hexdigest(),
        "payload_sha256": payload_sha,
        "rebuilt_payload_sha256": _canonical_sha256(dict(expected)),
        "tracked_payload_equal": True,
        "schema_version": actual.get("schema_version"),
        "status": actual.get("status"),
    }


def _validate_source_bindings(value: object) -> list[Mapping[str, Any]]:
    rows = list(_records(value, "source_bindings"))
    if not rows:
        raise WaveReadinessError("SOURCE_BINDINGS_EMPTY", "source_bindings cannot be empty")
    binding_ids: set[str] = set()
    commit_paths: set[tuple[str, str]] = set()
    for index, row in enumerate(rows):
        _exact_keys(row, _SOURCE_BINDING_KEYS, "SOURCE_BINDING_FIELDS")
        binding_id = _identifier(row["binding_id"], f"source_bindings[{index}].binding_id")
        if binding_id in binding_ids:
            raise WaveReadinessError("DUPLICATE_SOURCE_BINDING_ID", binding_id)
        binding_ids.add(binding_id)
        kind = _text(row["binding_kind"], f"source_bindings[{index}].binding_kind")
        if kind not in {"SOURCE", "REQUIREMENT", "GENERATED_STATE"}:
            raise WaveReadinessError("SOURCE_BINDING_KIND", kind)
        role = _text(row["commit_role"], f"source_bindings[{index}].commit_role")
        if role not in {"SOURCE_WAVE", "LANE_BASE"}:
            raise WaveReadinessError("SOURCE_BINDING_COMMIT_ROLE", role)
        path = _portable_path(row["path"], f"source_bindings[{index}].path")
        if (role, path) in commit_paths:
            raise WaveReadinessError("DUPLICATE_SOURCE_BINDING_PATH", f"{role}:{path}")
        commit_paths.add((role, path))
        _sha256(row["blob_sha256"], f"source_bindings[{index}].blob_sha256")
        for field in ("schema_version", "status"):
            field_value = row[field]
            if field_value is not None:
                _identifier(field_value, f"source_bindings[{index}].{field}")
    _require_canonical_record_order(rows, "binding_id", "source_bindings")
    return rows


def _validate_task_bindings(value: object) -> list[Mapping[str, Any]]:
    rows = list(_records(value, "task_bindings"))
    if not rows:
        raise WaveReadinessError("TASK_BINDINGS_EMPTY", "task_bindings cannot be empty")
    task_ids: set[str] = set()
    for index, row in enumerate(rows):
        _exact_keys(row, _TASK_BINDING_KEYS, "TASK_BINDING_FIELDS")
        task_id = _identifier(row["task_id"], f"task_bindings[{index}].task_id")
        if task_id in task_ids:
            raise WaveReadinessError("DUPLICATE_TASK_BINDING", task_id)
        task_ids.add(task_id)
        _portable_path(
            row["source_register_path"],
            f"task_bindings[{index}].source_register_path",
        )
        _sha256(
            row["source_register_blob_sha256"],
            f"task_bindings[{index}].source_register_blob_sha256",
        )
        _sha256(row["row_sha256"], f"task_bindings[{index}].row_sha256")
        priority = _text(row["priority"], f"task_bindings[{index}].priority")
        if re.fullmatch(r"P[0-3]", priority) is None:
            raise WaveReadinessError("TASK_PRIORITY", priority)
        _identifier(row["status"], f"task_bindings[{index}].status")
        _portable_path(
            row["shadow_fragment_path"],
            f"task_bindings[{index}].shadow_fragment_path",
        )
        _sha256(
            row["shadow_fragment_blob_sha256"],
            f"task_bindings[{index}].shadow_fragment_blob_sha256",
        )
        if row["shadow_fragment_schema_version"] != SHADOW_FRAGMENT_SCHEMA_VERSION:
            raise WaveReadinessError("TASK_FRAGMENT_SCHEMA", task_id)
        _sha256(
            row["shadow_fragment_checksum"],
            f"task_bindings[{index}].shadow_fragment_checksum",
        )
        _canonical_paths(
            row["requirement_paths"],
            f"task_bindings[{index}].requirement_paths",
        )
    _require_canonical_record_order(rows, "task_id", "task_bindings")
    return rows


def _validate_lease_authority(value: object) -> None:
    lease = _mapping(value, "lease_authority")
    _exact_keys(lease, _LEASE_AUTHORITY_KEYS, "LEASE_AUTHORITY_FIELDS")
    if (
        lease["kind"] != "NONE_AT_S0_MANUAL_ASSIGNMENT"
        or lease["lease_namespace_created"] is not False
        or lease["lease_acquisition_allowed"] is not False
        or type(lease["active_shared_path_lease_count"]) is not int
        or lease["active_shared_path_lease_count"] != 0
    ):
        raise WaveReadinessError(
            "LEASE_AUTHORITY_UNSAFE",
            "manual S0 requires no namespace, acquisition authority, or active leases",
        )


def _validate_assignment_control(value: object, *, owner: str) -> None:
    assignment = _mapping(value, "assignment_control")
    _exact_keys(assignment, _ASSIGNMENT_CONTROL_KEYS, "ASSIGNMENT_CONTROL_FIELDS")
    if (
        assignment["mode"] != "MANUAL_COORDINATOR_AFTER_CARRIER_PUSH"
        or assignment["authority"] != owner
        or assignment["worker_assignment_allowed_after_s0_pass"] is not True
        or assignment["carrier_commit_push_required"] is not True
        or assignment["automatic_command_dispatch"] is not False
        or assignment["automatic_merge"] is not False
    ):
        raise WaveReadinessError(
            "ASSIGNMENT_CONTROL_UNSAFE",
            "only coordinator manual assignment after carrier push is allowed",
        )


def _validate_safety(value: object) -> None:
    safety = _mapping(value, "safety")
    _exact_keys(safety, _SAFETY_KEYS, "SAFETY_FIELDS")
    for field in (
        "dispatch_allowed",
        "automatic_command_dispatch",
        "lease_acquisition_allowed",
        "automatic_merge_allowed",
        "consumer_cutover_allowed",
        "task_registry_mutation_allowed",
    ):
        if safety[field] is not False:
            raise WaveReadinessError("SAFETY_BOUNDARY", f"{field} must be false")
    if safety["production_effect"] != "none" or safety["broker_action"] != "none":
        raise WaveReadinessError(
            "SAFETY_EFFECT",
            "production_effect and broker_action must equal none",
        )


def _validate_evidence_lane_plan(value: object) -> None:
    plan = _mapping(value, "lane_plan")
    _exact_keys(
        plan,
        frozenset(
            {
                "plan_id",
                "schema_version",
                "status",
                "current_base_commit",
                "max_parallel_domain_lanes",
                "manifest_sha256",
                "waves",
                "conflicts",
                "blocking_issues",
                "dispatch_allowed",
                "lease_acquisition_allowed",
                "task_registry_mutated",
                "production_effect",
            }
        ),
        "LANE_PLAN_FIELDS",
    )
    _identifier(plan["plan_id"], "lane_plan.plan_id")
    if plan["schema_version"] != "lane_plan.v1" or plan["status"] != "PASS":
        raise WaveReadinessError("LANE_PLAN_STATUS", "lane plan schema/status must pass")
    _commit(plan["current_base_commit"], "lane_plan.current_base_commit")
    if type(plan["max_parallel_domain_lanes"]) is not int or plan["max_parallel_domain_lanes"] != 2:
        raise WaveReadinessError("LANE_PLAN_CAPACITY", "must equal 2")
    hashes = _records(plan["manifest_sha256"], "lane_plan.manifest_sha256")
    for row in hashes:
        _exact_keys(
            row,
            frozenset({"change_id", "sha256"}),
            "LANE_PLAN_MANIFEST_HASH_FIELDS",
        )
        _identifier(row["change_id"], "lane_plan.change_id")
        _sha256(row["sha256"], "lane_plan.manifest_sha256")
    _require_canonical_record_order(hashes, "change_id", "lane_plan.manifest_sha256")
    waves = _records(plan["waves"], "lane_plan.waves")
    if len(waves) != 2:
        raise WaveReadinessError("LANE_PLAN_WAVE_COUNT", str(len(waves)))
    expected_kinds = ("DOMAIN", "COORDINATOR")
    for index, (wave, expected_kind) in enumerate(zip(waves, expected_kinds, strict=True)):
        _exact_keys(
            wave,
            frozenset({"wave_id", "kind", "assignments"}),
            "LANE_PLAN_WAVE_FIELDS",
        )
        _identifier(wave["wave_id"], f"lane_plan.waves[{index}].wave_id")
        if wave["kind"] != expected_kind:
            raise WaveReadinessError("LANE_PLAN_WAVE_KIND", f"{index}:{wave['kind']}")
        assignments = _records(wave["assignments"], f"lane_plan.waves[{index}].assignments")
        expected_count = 2 if index == 0 else 1
        if len(assignments) != expected_count:
            raise WaveReadinessError(
                "LANE_PLAN_ASSIGNMENT_COUNT",
                f"{index}:expected={expected_count} actual={len(assignments)}",
            )
        for assignment in assignments:
            _exact_keys(
                assignment,
                frozenset({"lane_id", "change_id", "manifest_sha256"}),
                "LANE_PLAN_ASSIGNMENT_FIELDS",
            )
            _identifier(assignment["lane_id"], "lane_plan.lane_id")
            _identifier(assignment["change_id"], "lane_plan.change_id")
            _sha256(
                assignment["manifest_sha256"],
                "lane_plan.assignment.manifest_sha256",
            )
    conflicts = _records(plan["conflicts"], "lane_plan.conflicts")
    for conflict in conflicts:
        _exact_keys(
            conflict,
            frozenset({"code", "change_ids", "resource", "serializable"}),
            "LANE_PLAN_CONFLICT_FIELDS",
        )
        _identifier(conflict["code"], "lane_plan.conflict.code")
        change_ids = _sequence(conflict["change_ids"], "lane_plan.conflict.change_ids")
        if len(change_ids) != 2:
            raise WaveReadinessError("LANE_PLAN_CONFLICT_CHANGE_IDS", str(change_ids))
        for change_id in change_ids:
            _identifier(change_id, "lane_plan.conflict.change_id")
        _text(conflict["resource"], "lane_plan.conflict.resource")
        if type(conflict["serializable"]) is not bool:
            raise WaveReadinessError(
                "LANE_PLAN_CONFLICT_SERIALIZABLE", str(conflict["serializable"])
            )
    if _sequence(plan["blocking_issues"], "lane_plan.blocking_issues"):
        raise WaveReadinessError("LANE_PLAN_BLOCKING_ISSUES", "PASS plan cannot have blockers")
    if (
        plan["dispatch_allowed"] is not False
        or plan["lease_acquisition_allowed"] is not False
        or plan["task_registry_mutated"] is not False
        or plan["production_effect"] != "none"
    ):
        raise WaveReadinessError(
            "LANE_PLAN_AUTHORITY", "lane plan cannot grant execution authority"
        )


def _validate_evidence_structure(payload: Mapping[str, Any]) -> None:
    _reject_non_finite(payload, "evidence")
    _exact_keys(payload, _EVIDENCE_KEYS, "EVIDENCE_FIELDS")
    if payload["schema_version"] != EVIDENCE_SCHEMA_VERSION:
        raise WaveReadinessError("EVIDENCE_SCHEMA", f"expected={EVIDENCE_SCHEMA_VERSION}")
    if payload["status"] != "PASS":
        raise WaveReadinessError("EVIDENCE_STATUS", "status must equal PASS")
    _identifier(payload["wave"], "wave")
    _identifier(payload["gate"], "gate")
    _text(payload["owner"], "owner")
    policy_binding = _mapping(payload["policy_binding"], "policy_binding")
    _exact_keys(policy_binding, frozenset({"path", "sha256"}), "POLICY_BINDING_FIELDS")
    _portable_path(policy_binding["path"], "policy_binding.path")
    _sha256(policy_binding["sha256"], "policy_binding.sha256")
    producer = _mapping(payload["producer"], "producer")
    _exact_keys(
        producer,
        frozenset(
            {
                "schema_version",
                "module_path",
                "module_blob_sha256",
                "cli_path",
                "cli_blob_sha256",
            }
        ),
        "PRODUCER_FIELDS",
    )
    if producer["schema_version"] != PRODUCER_SCHEMA_VERSION:
        raise WaveReadinessError("PRODUCER_SCHEMA", str(producer["schema_version"]))
    _portable_path(producer["module_path"], "producer.module_path")
    _portable_path(producer["cli_path"], "producer.cli_path")
    _sha256(producer["module_blob_sha256"], "producer.module_blob_sha256")
    _sha256(producer["cli_blob_sha256"], "producer.cli_blob_sha256")

    source_wave = _mapping(payload["source_wave"], "source_wave")
    _exact_keys(source_wave, _SOURCE_WAVE_KEYS, "SOURCE_WAVE_FIELDS")
    _identifier(source_wave["wave_id"], "source_wave.wave_id")
    _commit(source_wave["commit"], "source_wave.commit")
    _commit(source_wave["tree_sha1"], "source_wave.tree_sha1")
    lane = _mapping(payload["lane_base"], "lane_base")
    _exact_keys(
        lane,
        frozenset(
            {
                "commit",
                "tree_sha1",
                "branch",
                "remote_ref",
                "build_ref_state",
                "source_wave_is_ancestor",
                "carrier_commit_recorded",
            }
        ),
        "EVIDENCE_LANE_BASE_FIELDS",
    )
    _commit(lane["commit"], "lane_base.commit")
    _commit(lane["tree_sha1"], "lane_base.tree_sha1")
    _git_ref(lane["branch"], "lane_base.branch")
    _remote_ref(lane["remote_ref"], "lane_base.remote_ref")
    refs = _mapping(lane["build_ref_state"], "lane_base.build_ref_state")
    _exact_keys(
        refs,
        frozenset({"HEAD", "FETCH_HEAD", "remote"}),
        "BUILD_REF_STATE_FIELDS",
    )
    for field in ("HEAD", "FETCH_HEAD", "remote"):
        _commit(refs[field], f"build_ref_state.{field}")
    if lane["source_wave_is_ancestor"] is not True or lane["carrier_commit_recorded"] is not False:
        raise WaveReadinessError(
            "EVIDENCE_ANCESTRY_AUTHORITY",
            "source ancestry must pass and carrier commit must not be recorded",
        )
    _validate_source_bindings(payload["source_bindings"])
    _validate_task_bindings(payload["task_bindings"])

    replay = _mapping(payload["generated_state_replay"], "generated_state_replay")
    _exact_keys(
        replay,
        frozenset(
            {
                "status",
                "snapshot_commit",
                "snapshot_tree_sha1",
                "snapshot_source",
                "snapshot_archive_paths",
                "snapshot_excluded_paths",
                "tracked_payload_equality",
                "module_manifest_rebuilt",
                "test_manifest_rebuilt",
                "aggregate_index_rebuilt",
                "task_baseline_rebuilt",
                "task_shadow_index_rebuilt",
                "task_fragment_count",
                "task_fragment_set_sha256",
                "artifacts",
                "main_worktree_business_bytes_read",
            }
        ),
        "GENERATED_REPLAY_FIELDS",
    )
    if replay["status"] != "PASS":
        raise WaveReadinessError("GENERATED_REPLAY_STATUS", str(replay["status"]))
    if replay["snapshot_source"] != "GIT_ARCHIVE_C_ALLOWLIST_ONLY":
        raise WaveReadinessError(
            "GENERATED_REPLAY_SNAPSHOT_SOURCE",
            str(replay["snapshot_source"]),
        )
    _commit(replay["snapshot_commit"], "generated_state_replay.snapshot_commit")
    _commit(
        replay["snapshot_tree_sha1"],
        "generated_state_replay.snapshot_tree_sha1",
    )
    _sha256(
        replay["task_fragment_set_sha256"],
        "generated_state_replay.task_fragment_set_sha256",
    )
    archive_paths = _canonical_paths(
        replay["snapshot_archive_paths"],
        "generated_state_replay.snapshot_archive_paths",
    )
    excluded_paths = _canonical_paths(
        replay["snapshot_excluded_paths"],
        "generated_state_replay.snapshot_excluded_paths",
    )
    _validate_snapshot_path_separation(
        archive_paths=archive_paths,
        excluded_paths=excluded_paths,
    )
    _integer(
        replay["task_fragment_count"],
        "generated_state_replay.task_fragment_count",
        minimum=1,
    )
    for field in (
        "tracked_payload_equality",
        "module_manifest_rebuilt",
        "test_manifest_rebuilt",
        "aggregate_index_rebuilt",
        "task_baseline_rebuilt",
        "task_shadow_index_rebuilt",
    ):
        if replay[field] is not True:
            raise WaveReadinessError("GENERATED_REPLAY_BOUNDARY", field)
    if replay["main_worktree_business_bytes_read"] is not False:
        raise WaveReadinessError("WORKTREE_BUSINESS_BYTES_READ", "must remain false")
    artifacts = _records(replay["artifacts"], "generated_state_replay.artifacts")
    for row in artifacts:
        _exact_keys(
            row,
            frozenset(
                {
                    "path",
                    "blob_sha256",
                    "payload_sha256",
                    "rebuilt_payload_sha256",
                    "tracked_payload_equal",
                    "schema_version",
                    "status",
                }
            ),
            "GENERATED_ARTIFACT_FIELDS",
        )
        _portable_path(row["path"], "generated artifact path")
        _sha256(row["blob_sha256"], "generated artifact blob_sha256")
        _sha256(row["payload_sha256"], "generated artifact payload_sha256")
        _sha256(
            row["rebuilt_payload_sha256"],
            "generated artifact rebuilt_payload_sha256",
        )
        if (
            row["tracked_payload_equal"] is not True
            or row["payload_sha256"] != row["rebuilt_payload_sha256"]
        ):
            raise WaveReadinessError("GENERATED_ARTIFACT_REPLAY", str(row["path"]))
    _require_canonical_record_order(artifacts, "path", "generated_state_replay.artifacts")

    manifests = _records(payload["change_manifests"], "change_manifests")
    for raw in manifests:
        try:
            parse_change_manifest(raw)
        except ParallelControlError as exc:
            raise WaveReadinessError(f"MANIFEST_{exc.code}", exc.message) from exc
    _validate_evidence_lane_plan(payload["lane_plan"])
    worktree = _mapping(payload["worktree_guard"], "worktree_guard")
    _exact_keys(
        worktree,
        frozenset(
            {
                "status",
                "known_unrelated_paths",
                "pending_output_paths",
                "unexpected_paths",
                "known_unrelated_path_bytes_read",
                "pending_output_presence_recorded",
            }
        ),
        "EVIDENCE_WORKTREE_FIELDS",
    )
    if (
        worktree["status"] != "PASS"
        or worktree["unexpected_paths"] != []
        or worktree["known_unrelated_path_bytes_read"] is not False
        or worktree["pending_output_presence_recorded"] is not False
    ):
        raise WaveReadinessError("EVIDENCE_WORKTREE_GUARD", "worktree guard must be non-invasive")
    _validate_worktree_allowlist_shapes(
        {
            "known_unrelated_paths": worktree["known_unrelated_paths"],
            "pending_output_paths": worktree["pending_output_paths"],
        }
    )
    _validate_lease_authority(payload["lease_authority"])
    _validate_assignment_control(payload["assignment_control"], owner=str(payload["owner"]))
    _validate_safety(payload["safety"])
    checksum = _sha256(payload["evidence_checksum"], "evidence_checksum")
    if checksum != calculate_evidence_checksum(payload):
        raise WaveReadinessError("EVIDENCE_CHECKSUM", "checksum mismatch")


def _snapshot_archive_paths(
    *,
    project_root: Path,
    lane_commit: str,
    policy: Mapping[str, Any],
) -> tuple[str, ...]:
    generated = _mapping(policy["generated_state"], "generated_state")
    generated_paths = {
        _portable_path(generated[field], f"generated_state.{field}")
        for field in _GENERATED_STATE_KEYS
    }
    ownership_path = _portable_path(
        generated["ownership_policy_path"],
        "generated_state.ownership_policy_path",
    )
    ownership = _mapping(
        _load_mapping_bytes(
            git_blob_bytes(project_root, lane_commit, ownership_path),
            ownership_path,
        ),
        "ownership policy",
    )
    aggregate_targets = _records(
        ownership.get("aggregate_targets"),
        "ownership_policy.aggregate_targets",
    )
    paths = {
        *_SNAPSHOT_SCAN_ROOTS,
        ACTIVE_REGISTER_PATH,
        COMPLETED_REGISTER_PATH,
        "config/architecture/fragments",
        *generated_paths,
    }
    for index, target in enumerate(aggregate_targets):
        for field in ("current_path", "fragment_root"):
            paths.add(
                _portable_path(
                    target.get(field),
                    f"ownership_policy.aggregate_targets[{index}].{field}",
                )
            )
    minimal: list[str] = []
    for path in sorted(paths):
        if any(path == root or path.startswith(f"{root}/") for root in minimal):
            continue
        minimal.append(path)
    if not minimal:
        raise WaveReadinessError(
            "GIT_ARCHIVE_PATHS_EMPTY",
            "snapshot archive path allowlist cannot be empty",
        )
    return tuple(minimal)


def _validate_snapshot_path_separation(
    *,
    archive_paths: Sequence[str],
    excluded_paths: Sequence[str],
) -> None:
    archive_folded = tuple(path.casefold() for path in archive_paths)
    collisions = sorted(
        excluded
        for excluded in excluded_paths
        if any(
            excluded.casefold() == archive
            or excluded.casefold().startswith(f"{archive}/")
            or archive.startswith(f"{excluded.casefold()}/")
            for archive in archive_folded
        )
    )
    if collisions:
        raise WaveReadinessError(
            "GIT_ARCHIVE_EXCLUDED_PATH_COLLISION",
            ",".join(collisions),
        )


@contextmanager
def _git_commit_snapshot(
    project_root: Path,
    commit: str,
    *,
    archive_paths: Sequence[str],
    forbidden_paths: Sequence[str],
) -> Iterator[Path]:
    validated = _commit(commit, "snapshot.commit")
    paths = tuple(
        sorted({_portable_path(path, "snapshot.archive_paths") for path in archive_paths})
    )
    forbidden = tuple(
        sorted({_portable_path(path, "snapshot.forbidden_paths") for path in forbidden_paths})
    )
    if not paths:
        raise WaveReadinessError(
            "GIT_ARCHIVE_PATHS_EMPTY",
            "snapshot archive path allowlist cannot be empty",
        )
    _validate_snapshot_path_separation(
        archive_paths=paths,
        excluded_paths=forbidden,
    )
    with tempfile.TemporaryDirectory(prefix="aits-wave-readiness-") as raw_temp:
        temp_root = Path(raw_temp)
        archive_path = temp_root / "snapshot.tar"
        result = _git_process(
            project_root.resolve(),
            "archive",
            "--format=tar",
            f"--output={archive_path}",
            validated,
            *paths,
        )
        if result.returncode != 0:
            raise WaveReadinessError("GIT_ARCHIVE", f"{validated}:{_git_error(result)}")
        snapshot_root = temp_root / "snapshot"
        snapshot_root.mkdir()
        try:
            with tarfile.open(archive_path, mode="r:") as archive:
                _extract_validated_archive(
                    archive,
                    snapshot_root,
                    forbidden_paths=forbidden,
                )
        except (OSError, tarfile.TarError) as exc:
            raise WaveReadinessError("GIT_ARCHIVE_EXTRACT", validated) from exc
        yield snapshot_root


def _extract_validated_archive(
    archive: tarfile.TarFile,
    destination: Path,
    *,
    forbidden_paths: Sequence[str] = (),
) -> None:
    root = destination.resolve()
    if not root.is_dir() or any(root.iterdir()):
        raise WaveReadinessError(
            "GIT_ARCHIVE_DESTINATION",
            "snapshot destination must be an empty directory",
        )
    members = archive.getmembers()
    forbidden = tuple(
        sorted(
            {_portable_path(path, "archive.forbidden_paths").casefold() for path in forbidden_paths}
        )
    )
    validated: list[tuple[tarfile.TarInfo, str]] = []
    by_casefolded_path: dict[str, tuple[str, bool]] = {}
    for member in members:
        portable = _portable_archive_member(member.name)
        portable_folded = portable.casefold()
        if any(
            portable_folded == path or portable_folded.startswith(f"{path}/") for path in forbidden
        ):
            raise WaveReadinessError("GIT_ARCHIVE_FORBIDDEN_PATH", portable)
        if member.type not in _ALLOWED_TAR_MEMBER_TYPES:
            raise WaveReadinessError("GIT_ARCHIVE_ENTRY_TYPE", portable)
        is_directory = member.type == tarfile.DIRTYPE
        if is_directory != member.isdir() or (not is_directory and not member.isfile()):
            raise WaveReadinessError("GIT_ARCHIVE_ENTRY_TYPE", portable)
        if member.linkname:
            raise WaveReadinessError("GIT_ARCHIVE_LINK_METADATA", portable)
        if member.size < 0 or (is_directory and member.size != 0):
            raise WaveReadinessError("GIT_ARCHIVE_ENTRY_SIZE", portable)
        target = (root / PurePosixPath(portable)).resolve()
        if not target.is_relative_to(root):
            raise WaveReadinessError("GIT_ARCHIVE_ENTRY_PATH", portable)
        folded = portable.casefold()
        if folded in by_casefolded_path:
            previous = by_casefolded_path[folded][0]
            raise WaveReadinessError("GIT_ARCHIVE_DUPLICATE_PATH", f"{previous}:{portable}")
        by_casefolded_path[folded] = (portable, is_directory)
        validated.append((member, portable))
    for folded, (portable, _) in by_casefolded_path.items():
        parts = PurePosixPath(folded).parts
        for index in range(1, len(parts)):
            parent = PurePosixPath(*parts[:index]).as_posix()
            parent_record = by_casefolded_path.get(parent)
            if parent_record is not None and parent_record[1] is False:
                raise WaveReadinessError(
                    "GIT_ARCHIVE_FILE_PARENT", f"{parent_record[0]}:{portable}"
                )
    archive.extractall(
        path=root,
        members=[member for member, _ in validated],
        filter="data",
    )
    for member, portable in validated:
        target = (root / PurePosixPath(portable)).resolve()
        if member.isdir():
            if not target.is_dir() or target.is_symlink():
                raise WaveReadinessError("GIT_ARCHIVE_DIRECTORY_VERIFY", portable)
        elif not target.is_file() or target.is_symlink() or target.stat().st_size != member.size:
            raise WaveReadinessError("GIT_ARCHIVE_FILE_VERIFY", portable)


def extract_validated_archive(archive: tarfile.TarFile, destination: Path) -> None:
    """Extract an archive through the shared fail-closed path and member gate."""
    _extract_validated_archive(archive, destination)


def _load_mapping_bytes(raw: bytes, path: str) -> Mapping[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WaveReadinessError("SOURCE_BINDING_UTF8", path) from exc
    suffix = PurePosixPath(path).suffix.lower()
    if suffix == ".json":
        value = load_strict_json_text(text, label=path)
    elif suffix in {".yaml", ".yml"}:
        value = load_strict_yaml_text(text, label=path)
    else:
        raise WaveReadinessError("SOURCE_BINDING_STRUCTURED_SUFFIX", path)
    return _mapping(value, path)


def _snapshot_file(snapshot_root: Path, path: str) -> Path:
    portable = _portable_path(path, "snapshot.path")
    candidate = (snapshot_root / PurePosixPath(portable)).resolve()
    root = snapshot_root.resolve()
    if not candidate.is_relative_to(root) or not candidate.is_file():
        raise WaveReadinessError("SNAPSHOT_FILE_MISSING", portable)
    return candidate


def _snapshot_dir(snapshot_root: Path, path: str) -> Path:
    portable = _portable_path(path, "snapshot.path")
    candidate = (snapshot_root / PurePosixPath(portable)).resolve()
    root = snapshot_root.resolve()
    if not candidate.is_relative_to(root) or not candidate.is_dir():
        raise WaveReadinessError("SNAPSHOT_DIRECTORY_MISSING", portable)
    return candidate


def _portable_archive_member(value: str) -> str:
    text = value.removesuffix("/")
    return _portable_path(text, "archive.member")


def _current_branch(project_root: Path) -> str:
    result = _git_process(project_root.resolve(), "symbolic-ref", "--quiet", "--short", "HEAD")
    if result.returncode != 0:
        raise WaveReadinessError("GIT_BRANCH_UNAVAILABLE", _git_error(result))
    return _git_ref(
        result.stdout.decode("utf-8", errors="strict").strip(),
        "current_branch",
    )


def _git_process(project_root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", *args],
            cwd=project_root,
            check=False,
            capture_output=True,
            shell=False,
        )
    except OSError as exc:
        raise WaveReadinessError("GIT_EXECUTION", str(exc)) from exc


def _git_error(result: subprocess.CompletedProcess[bytes]) -> str:
    return (
        result.stderr.decode("utf-8", errors="replace").strip() or f"git exited {result.returncode}"
    )


def _decode_status_path(raw: bytes) -> str:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WaveReadinessError("GIT_STATUS_PATH_UTF8", repr(raw)) from exc
    return _portable_path(text, "git_status.path")


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise WaveReadinessError("JSON_DUPLICATE_KEY", key)
        result[key] = value
    return result


def _reject_non_finite(
    value: object,
    field: str,
    *,
    visiting: set[int] | None = None,
    visited: set[int] | None = None,
) -> None:
    active = visiting if visiting is not None else set()
    complete = visited if visited is not None else set()
    if isinstance(value, float) and not math.isfinite(value):
        raise WaveReadinessError("NON_FINITE_NUMBER", field)
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {
            "nan",
            "+nan",
            "-nan",
            "inf",
            "+inf",
            "-inf",
            "infinity",
            "+infinity",
            "-infinity",
        }:
            raise WaveReadinessError("NON_FINITE_NUMBER", field)
        if _EXPONENT_NUMBER_RE.fullmatch(value):
            try:
                parsed = float(value)
            except ValueError:
                parsed = 0.0
            if not math.isfinite(parsed):
                raise WaveReadinessError("NON_FINITE_NUMBER", field)
    is_mapping = isinstance(value, Mapping)
    is_sequence = isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
    if not is_mapping and not is_sequence:
        return
    identity = id(value)
    if identity in active:
        raise WaveReadinessError("YAML_CYCLIC_ALIAS", field)
    if identity in complete:
        return
    active.add(identity)
    try:
        if is_mapping:
            mapping = _mapping(value, field)
            for key, child in mapping.items():
                _reject_non_finite(
                    child,
                    f"{field}.{key}",
                    visiting=active,
                    visited=complete,
                )
        else:
            sequence = _sequence(value, field)
            for index, child in enumerate(sequence):
                _reject_non_finite(
                    child,
                    f"{field}[{index}]",
                    visiting=active,
                    visited=complete,
                )
    finally:
        active.remove(identity)
    complete.add(identity)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WaveReadinessError("MAPPING_REQUIRED", field)
    if not all(isinstance(key, str) for key in value):
        raise WaveReadinessError("NON_STRING_KEY", field)
    return value


def _records(value: object, field: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise WaveReadinessError("LIST_REQUIRED", field)
    return tuple(_mapping(row, field) for row in value)


def _sequence(value: object, field: str) -> tuple[object, ...]:
    if not isinstance(value, list):
        raise WaveReadinessError("LIST_REQUIRED", field)
    return tuple(value)


def _exact_keys(value: Mapping[str, Any], expected: frozenset[str], code: str) -> None:
    actual = frozenset(value)
    if actual != expected:
        raise WaveReadinessError(
            code,
            f"missing={sorted(expected - actual)} unknown={sorted(actual - expected)}",
        )


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise WaveReadinessError("TEXT_REQUIRED", f"{field} must be non-empty trimmed text")
    return value


def _identifier(value: object, field: str) -> str:
    text = _text(value, field)
    if _IDENTIFIER_RE.fullmatch(text) is None:
        raise WaveReadinessError("IDENTIFIER_INVALID", f"{field}:{text}")
    return text


def _commit(value: object, field: str) -> str:
    text = _text(value, field)
    if _COMMIT_RE.fullmatch(text) is None:
        raise WaveReadinessError("COMMIT_INVALID", f"{field}:{text}")
    return text


def _sha256(value: object, field: str) -> str:
    text = _text(value, field)
    if _SHA256_RE.fullmatch(text) is None:
        raise WaveReadinessError("SHA256_INVALID", f"{field}:{text}")
    return text


def _git_ref(value: object, field: str) -> str:
    text = _text(value, field)
    if (
        _REF_RE.fullmatch(text) is None
        or ".." in text
        or "//" in text
        or text.endswith(("/", "."))
        or text.startswith(".")
        or "@{" in text
    ):
        raise WaveReadinessError("GIT_REF_INVALID", f"{field}:{text}")
    return text


def _remote_ref(value: object, field: str) -> str:
    text = _git_ref(value, field)
    if text in {"HEAD", "FETCH_HEAD"} or text.startswith("refs/heads/") or "/" not in text:
        raise WaveReadinessError(
            "REMOTE_REF_INVALID",
            f"{field} must identify an explicit remote-tracking ref",
        )
    if text.startswith("refs/") and not text.startswith("refs/remotes/"):
        raise WaveReadinessError("REMOTE_REF_INVALID", f"{field}:{text}")
    return text


def _integer(value: object, field: str, *, minimum: int) -> int:
    if type(value) is not int or value < minimum:
        raise WaveReadinessError("INTEGER_INVALID", f"{field}:minimum={minimum}")
    return value


def _portable_path(value: object, field: str) -> str:
    text = _text(value, field)
    if (
        "\\" in text
        or text.endswith("/")
        or "\0" in text
        or ":" in text
        or any(ord(character) < 32 for character in text)
    ):
        raise WaveReadinessError("PATH_INVALID", f"{field}:{text}")
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.parts[0] == ".git"
        or path.as_posix() != text
        or any(
            part.endswith((" ", "."))
            or part.split(".", maxsplit=1)[0].upper() in _WINDOWS_RESERVED_NAMES
            for part in path.parts
        )
    ):
        raise WaveReadinessError("PATH_INVALID", f"{field}:{text}")
    return text


def _canonical_paths(value: object, field: str) -> tuple[str, ...]:
    paths = tuple(_portable_path(row, field) for row in _sequence(value, field))
    if len(paths) != len(set(paths)):
        raise WaveReadinessError("DUPLICATE_PATH", field)
    if paths != tuple(sorted(paths)):
        raise WaveReadinessError("PATH_ORDER", f"{field} must be sorted")
    return paths


def _require_canonical_record_order(
    rows: Sequence[Mapping[str, Any]], field: str, label: str
) -> None:
    values = [str(row[field]) for row in rows]
    if values != sorted(values):
        raise WaveReadinessError("RECORD_ORDER", f"{label} must be sorted by {field}")


def _require_canonical_manifest_order(
    rows: Sequence[Mapping[str, Any]],
) -> None:
    _require_canonical_record_order(rows, "change_id", "change_manifests")


def _relative_to_root(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise WaveReadinessError("PATH_OUTSIDE_PROJECT", str(path)) from exc


def _canonical_sha256(value: object) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise WaveReadinessError("CANONICAL_JSON", type(value).__name__) from exc
    return hashlib.sha256(encoded).hexdigest()


def _normalized_text_sha256(path: Path) -> str:
    raw = path.read_bytes()
    if b"\r" in raw.replace(b"\r\n", b""):
        raise WaveReadinessError("POLICY_LINE_ENDING", str(path))
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def _first_difference(left: object, right: object, path: str = "$") -> str:
    if type(left) is not type(right):
        return f"{path}:type {type(left).__name__}!={type(right).__name__}"
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        keys = sorted(set(left) | set(right))
        for key in keys:
            if key not in left:
                return f"{path}.{key}:missing-left"
            if key not in right:
                return f"{path}.{key}:missing-right"
            difference = _first_difference(left[key], right[key], f"{path}.{key}")
            if difference:
                return difference
        return ""
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return f"{path}:length {len(left)}!={len(right)}"
        for index, (left_item, right_item) in enumerate(zip(left, right, strict=True)):
            difference = _first_difference(left_item, right_item, f"{path}[{index}]")
            if difference:
                return difference
        return ""
    return "" if left == right else f"{path}:{left!r}!={right!r}"


__all__ = [
    "EVIDENCE_SCHEMA_VERSION",
    "GENERATOR_CLI_PATH",
    "GENERATOR_MODULE_PATH",
    "POLICY_SCHEMA_VERSION",
    "PRODUCER_SCHEMA_VERSION",
    "WaveReadinessError",
    "assert_worktree_guard",
    "build_wave_readiness_evidence",
    "calculate_evidence_checksum",
    "canonical_evidence_bytes",
    "get_worktree_dirty_paths",
    "git_blob_bytes",
    "git_blob_sha256",
    "git_commit_exists",
    "git_commit_tree",
    "git_is_ancestor",
    "git_resolve_ref",
    "load_strict_json_path",
    "load_strict_json_text",
    "load_strict_yaml_path",
    "load_strict_yaml_text",
    "load_wave_readiness_evidence",
    "load_wave_readiness_policy",
    "validate_wave_readiness_evidence",
    "validate_wave_readiness_policy",
]
