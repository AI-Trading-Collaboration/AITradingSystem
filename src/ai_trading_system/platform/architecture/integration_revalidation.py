from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from ai_trading_system.platform.architecture.checkout_guard import (
    collect_checkout_dirty_paths,
)
from ai_trading_system.platform.architecture.parallel_control import (
    ChangeManifest,
    ContractAccess,
    ContractClaim,
    parse_change_manifest,
)
from ai_trading_system.platform.artifacts.writer import write_json_atomic
from ai_trading_system.yaml_loader import (
    StrictYamlError,
    StrictYamlOptions,
    load_strict_yaml_text,
)

INTEGRATION_REVALIDATION_POLICY_SCHEMA_VERSION = (
    "arch_005_integration_revalidation_policy.v1"
)
INTEGRATION_REVALIDATION_PLAN_SCHEMA_VERSION = "integration_revalidation_plan.v1"
DEFAULT_INTEGRATION_REVALIDATION_POLICY_PATH = (
    Path(__file__).resolve().parents[4]
    / "config"
    / "architecture"
    / "arch_005_integration_revalidation.yaml"
)

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DECISIONS = {
    "READY_FOR_SINGLE_INTEGRATION_CANDIDATE",
    "RECONCILIATION_REQUIRED",
    "SERIAL_CONTRACT_WAVE_REQUIRED",
    "BLOCKED",
}
_STRICT_POLICY_YAML_OPTIONS = StrictYamlOptions(
    key_policy="STRING",
    flatten_mapping=False,
    reject_non_finite=False,
)


class IntegrationRevalidationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class IntegrationRevalidationPolicy:
    known_unrelated_exclusions: tuple[str, ...]
    coordinator_refreshable_scopes: tuple[str, ...]
    contract_sensitive_scopes: tuple[str, ...]
    final_validation_tiers: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": INTEGRATION_REVALIDATION_POLICY_SCHEMA_VERSION,
            "known_unrelated_exclusions": list(self.known_unrelated_exclusions),
            "coordinator_refreshable_scopes": list(
                self.coordinator_refreshable_scopes
            ),
            "contract_sensitive_scopes": list(self.contract_sensitive_scopes),
            "final_validation_tiers": list(self.final_validation_tiers),
        }

    @property
    def sha256(self) -> str:
        return _canonical_sha256(self.to_dict())


@dataclass(frozen=True)
class GitDeltaEntry:
    status: str
    paths: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {"status": self.status, "paths": list(self.paths)}


@dataclass(frozen=True, order=True)
class PathOverlap:
    task_path: str
    mainline_path: str
    classification: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "task_path": self.task_path,
            "mainline_path": self.mainline_path,
            "classification": self.classification,
            "reason": self.reason,
        }


def load_integration_revalidation_policy(
    path: Path = DEFAULT_INTEGRATION_REVALIDATION_POLICY_PATH,
) -> IntegrationRevalidationPolicy:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise IntegrationRevalidationError("POLICY_READ_FAILED", str(exc)) from exc
    try:
        raw = load_strict_yaml_text(
            text,
            options=_STRICT_POLICY_YAML_OPTIONS,
            label=str(path),
        )
    except StrictYamlError as exc:
        if exc.code == "DUPLICATE_KEY":
            raise IntegrationRevalidationError(
                "POLICY_YAML_DUPLICATE_KEY",
                _strict_duplicate_key(exc.detail),
            ) from exc
        if exc.code == "NON_STRING_KEY":
            raise IntegrationRevalidationError(
                "POLICY_YAML_NON_STRING_KEY",
                exc.detail,
            ) from exc
        detail = str(exc.__cause__) if exc.__cause__ is not None else exc.detail
        raise IntegrationRevalidationError("POLICY_READ_FAILED", detail) from exc
    if not isinstance(raw, Mapping):
        raise IntegrationRevalidationError("POLICY_ROOT", "policy must be a mapping")
    expected = {
        "schema_version",
        "known_unrelated_exclusions",
        "coordinator_refreshable_scopes",
        "contract_sensitive_scopes",
        "final_validation_tiers",
    }
    _require_exact_keys(raw, expected, "POLICY_FIELDS")
    if raw["schema_version"] != INTEGRATION_REVALIDATION_POLICY_SCHEMA_VERSION:
        raise IntegrationRevalidationError(
            "POLICY_SCHEMA",
            f"expected {INTEGRATION_REVALIDATION_POLICY_SCHEMA_VERSION}",
        )
    exclusions = _unique_paths(
        raw["known_unrelated_exclusions"],
        "known_unrelated_exclusions",
    )
    refreshable = _unique_paths(
        raw["coordinator_refreshable_scopes"],
        "coordinator_refreshable_scopes",
    )
    contract_sensitive = _unique_paths(
        raw["contract_sensitive_scopes"],
        "contract_sensitive_scopes",
    )
    tiers = _unique_strings(raw["final_validation_tiers"], "final_validation_tiers")
    if not exclusions:
        raise IntegrationRevalidationError(
            "POLICY_EXCLUSIONS_EMPTY",
            "known_unrelated_exclusions cannot be empty",
        )
    if not tiers:
        raise IntegrationRevalidationError(
            "POLICY_VALIDATION_EMPTY",
            "final_validation_tiers cannot be empty",
        )
    for excluded in exclusions:
        if any(_paths_overlap(excluded, scope) for scope in (*refreshable, *contract_sensitive)):
            raise IntegrationRevalidationError(
                "POLICY_EXCLUSION_SCOPE_OVERLAP",
                excluded,
            )
    for refresh_scope in refreshable:
        if any(_paths_overlap(refresh_scope, scope) for scope in contract_sensitive):
            raise IntegrationRevalidationError(
                "POLICY_SCOPE_CONFLICT",
                refresh_scope,
            )
    return IntegrationRevalidationPolicy(
        known_unrelated_exclusions=exclusions,
        coordinator_refreshable_scopes=refreshable,
        contract_sensitive_scopes=contract_sensitive,
        final_validation_tiers=tiers,
    )


def _strict_duplicate_key(detail: str) -> str:
    key_repr, separator, line = detail.removeprefix("key=").rpartition(" line=")
    if not separator or not line.isdigit():
        return detail
    try:
        key = ast.literal_eval(key_repr)
    except (SyntaxError, ValueError):
        return detail
    return key if isinstance(key, str) else detail


def build_integration_revalidation_plan(
    *,
    repository: Path,
    frozen_base: str,
    lane_head: str,
    latest_main: str,
    manifest: ChangeManifest | Mapping[str, Any],
    policy: IntegrationRevalidationPolicy | None = None,
    policy_path: Path = DEFAULT_INTEGRATION_REVALIDATION_POLICY_PATH,
    mainline_contract_claims: Sequence[ContractClaim | Mapping[str, Any]] = (),
) -> dict[str, object]:
    active_policy = policy or load_integration_revalidation_policy(policy_path)
    parsed_manifest = (
        manifest if isinstance(manifest, ChangeManifest) else parse_change_manifest(manifest)
    )
    base = _commit(frozen_base, "frozen_base")
    lane = _commit(lane_head, "lane_head")
    main = _commit(latest_main, "latest_main")
    claims = _parse_contract_claims(mainline_contract_claims)
    root, common_dir, identity_before = _resolve_repository_identity(repository)
    resolved_base = _resolve_commit(root, base)
    resolved_lane = _resolve_commit(root, lane)
    resolved_main = _resolve_commit(root, main)

    blockers: list[dict[str, str]] = []
    if parsed_manifest.base_commit != base:
        blockers.append(
            _issue(
                "MANIFEST_BASE_MISMATCH",
                f"manifest={parsed_manifest.base_commit}; frozen_base={base}",
            )
        )
    if lane == main:
        blockers.append(_issue("LANE_EQUALS_LATEST_MAIN", lane))
    base_to_lane = _is_ancestor(root, base, lane)
    base_to_main = _is_ancestor(root, base, main)
    if not base_to_lane:
        blockers.append(_issue("BASE_NOT_LANE_ANCESTOR", f"{base}..{lane}"))
    if not base_to_main:
        blockers.append(_issue("BASE_NOT_MAIN_ANCESTOR", f"{base}..{main}"))

    dirty_paths = collect_checkout_dirty_paths(
        root,
        exclusions=active_policy.known_unrelated_exclusions,
    )
    if dirty_paths:
        blockers.append(_issue("REPOSITORY_DIRTY", ",".join(dirty_paths)))

    task_entries: tuple[GitDeltaEntry, ...] = ()
    mainline_entries: tuple[GitDeltaEntry, ...] = ()
    if base_to_lane:
        task_entries = _collect_delta_entries(
            root,
            base,
            lane,
            exclusions=active_policy.known_unrelated_exclusions,
        )
    if base_to_main:
        mainline_entries = _collect_delta_entries(
            root,
            base,
            main,
            exclusions=active_policy.known_unrelated_exclusions,
        )

    task_paths = _entry_paths(task_entries)
    mainline_paths = _entry_paths(mainline_entries)
    declared_owned = parsed_manifest.owned_paths
    declared_shared = parsed_manifest.shared_paths
    declared_paths = (*declared_owned, *declared_shared)
    undeclared_task_paths = tuple(
        path for path in task_paths if not _covered_by_scope(path, declared_paths)
    )
    for path in undeclared_task_paths:
        blockers.append(_issue("UNDECLARED_TASK_PATH", path))

    overlap_rows = _classify_overlaps(
        task_paths=task_paths,
        mainline_paths=mainline_paths,
        declared_shared_paths=declared_shared,
        policy=active_policy,
    )
    path_classifications = _classify_all_paths(
        task_paths=task_paths,
        mainline_paths=mainline_paths,
        overlaps=overlap_rows,
    )
    contract_conflicts = _contract_conflicts(parsed_manifest.contract_claims, claims)

    root_after, common_dir_after, identity_after = _resolve_repository_identity(root)
    if (
        root_after != root
        or common_dir_after != common_dir
        or identity_after != identity_before
    ):
        blockers.append(
            _issue(
                "REPOSITORY_IDENTITY_DRIFT",
                f"before={identity_before}; after={identity_after}",
            )
        )
    if (
        _resolve_commit(root, base) != resolved_base
        or _resolve_commit(root, lane) != resolved_lane
        or _resolve_commit(root, main) != resolved_main
    ):
        blockers.append(_issue("COMMIT_IDENTITY_DRIFT", "commit resolution changed"))

    overlap_classes = {row.classification for row in overlap_rows}
    if blockers:
        decision = "BLOCKED"
    elif contract_conflicts or "CONTRACT_SENSITIVE_OVERLAP" in overlap_classes:
        decision = "SERIAL_CONTRACT_WAVE_REQUIRED"
    elif "DOMAIN_OVERLAP" in overlap_classes:
        decision = "RECONCILIATION_REQUIRED"
    else:
        decision = "READY_FOR_SINGLE_INTEGRATION_CANDIDATE"

    plan_body: dict[str, object] = {
        "schema_version": INTEGRATION_REVALIDATION_PLAN_SCHEMA_VERSION,
        "repository": {
            "toplevel": root.as_posix(),
            "git_common_dir": common_dir.as_posix(),
            "identity": identity_before,
        },
        "frozen_base": base,
        "lane_head": lane,
        "latest_main": main,
        "manifest_sha256": parsed_manifest.sha256,
        "policy_sha256": active_policy.sha256,
        "mainline_contract_claims": [
            _contract_claim_dict(claim) for claim in claims
        ],
        "ancestry": {
            "base_is_lane_ancestor": base_to_lane,
            "base_is_latest_main_ancestor": base_to_main,
        },
        "dirty_paths": list(dirty_paths),
        "task_delta": [entry.to_dict() for entry in task_entries],
        "mainline_delta": [entry.to_dict() for entry in mainline_entries],
        "undeclared_task_paths": list(undeclared_task_paths),
        "overlaps": [row.to_dict() for row in overlap_rows],
        "path_classifications": list(path_classifications),
        "contract_conflicts": list(contract_conflicts),
        "blockers": blockers,
        "decision": decision,
        "required_next_stage": _next_stage(decision),
        "lane_focused_evidence_reuse_allowed": decision
        == "READY_FOR_SINGLE_INTEGRATION_CANDIDATE",
        "final_validation_tiers": list(active_policy.final_validation_tiers),
        "task_branch_rebuild_required": decision
        == "SERIAL_CONTRACT_WAVE_REQUIRED",
        "candidate_creation_allowed": decision
        == "READY_FOR_SINGLE_INTEGRATION_CANDIDATE",
        "reviewed_reconciliation_required": decision == "RECONCILIATION_REQUIRED",
        "automatic_git_mutation_allowed": False,
        "automatic_cleanup_allowed": False,
        "task_registry_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    checksum = _canonical_sha256(plan_body)
    return {
        **plan_body,
        "plan_id": f"integration-revalidation-{checksum[:20]}",
        "plan_sha256": checksum,
    }


def validate_integration_revalidation_plan(
    payload: Mapping[str, Any],
    *,
    repository: Path,
    manifest: ChangeManifest | Mapping[str, Any],
    policy: IntegrationRevalidationPolicy | None = None,
    policy_path: Path = DEFAULT_INTEGRATION_REVALIDATION_POLICY_PATH,
) -> None:
    expected_keys = {
        "schema_version",
        "repository",
        "frozen_base",
        "lane_head",
        "latest_main",
        "manifest_sha256",
        "policy_sha256",
        "mainline_contract_claims",
        "ancestry",
        "dirty_paths",
        "task_delta",
        "mainline_delta",
        "undeclared_task_paths",
        "overlaps",
        "path_classifications",
        "contract_conflicts",
        "blockers",
        "decision",
        "required_next_stage",
        "lane_focused_evidence_reuse_allowed",
        "final_validation_tiers",
        "task_branch_rebuild_required",
        "candidate_creation_allowed",
        "reviewed_reconciliation_required",
        "automatic_git_mutation_allowed",
        "automatic_cleanup_allowed",
        "task_registry_mutated",
        "production_effect",
        "broker_action",
        "plan_id",
        "plan_sha256",
    }
    _require_exact_keys(payload, expected_keys, "PLAN_FIELDS")
    if payload["schema_version"] != INTEGRATION_REVALIDATION_PLAN_SCHEMA_VERSION:
        raise IntegrationRevalidationError(
            "PLAN_SCHEMA",
            f"expected {INTEGRATION_REVALIDATION_PLAN_SCHEMA_VERSION}",
        )
    if payload["decision"] not in _DECISIONS:
        raise IntegrationRevalidationError("PLAN_DECISION", str(payload["decision"]))
    _sha256(payload["manifest_sha256"], "manifest_sha256")
    _sha256(payload["policy_sha256"], "policy_sha256")
    plan_sha256 = _sha256(payload["plan_sha256"], "plan_sha256")
    body = {key: value for key, value in payload.items() if key not in {"plan_id", "plan_sha256"}}
    expected_checksum = _canonical_sha256(body)
    if plan_sha256 != expected_checksum:
        raise IntegrationRevalidationError(
            "PLAN_CHECKSUM",
            f"expected {expected_checksum}",
        )
    if payload["plan_id"] != f"integration-revalidation-{expected_checksum[:20]}":
        raise IntegrationRevalidationError("PLAN_ID", str(payload["plan_id"]))
    claims_raw = payload["mainline_contract_claims"]
    if not isinstance(claims_raw, list):
        raise IntegrationRevalidationError(
            "PLAN_CONTRACT_CLAIMS",
            "mainline_contract_claims must be a list",
        )
    rebuilt = build_integration_revalidation_plan(
        repository=repository,
        frozen_base=_required_text(payload["frozen_base"], "frozen_base"),
        lane_head=_required_text(payload["lane_head"], "lane_head"),
        latest_main=_required_text(payload["latest_main"], "latest_main"),
        manifest=manifest,
        policy=policy,
        policy_path=policy_path,
        mainline_contract_claims=claims_raw,
    )
    if rebuilt != dict(payload):
        raise IntegrationRevalidationError(
            "PLAN_REBUILD_MISMATCH",
            "payload does not match repository-derived plan",
        )


def write_integration_revalidation_plan(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, dict(payload))


def _resolve_repository_identity(repository: Path) -> tuple[Path, Path, str]:
    requested = repository.resolve()
    top = Path(_run_git(requested, "rev-parse", "--show-toplevel")).resolve()
    if requested != top:
        raise IntegrationRevalidationError(
            "REPOSITORY_NOT_TOPLEVEL",
            f"requested={requested}; toplevel={top}",
        )
    raw_common = Path(_run_git(top, "rev-parse", "--git-common-dir"))
    common = (top / raw_common).resolve() if not raw_common.is_absolute() else raw_common.resolve()
    identity = _canonical_sha256(
        {"toplevel": top.as_posix(), "git_common_dir": common.as_posix()}
    )
    return top, common, identity


def _resolve_commit(repository: Path, commit: str) -> str:
    resolved = _run_git(repository, "rev-parse", "--verify", f"{commit}^{{commit}}")
    if resolved != commit:
        raise IntegrationRevalidationError(
            "COMMIT_RESOLUTION",
            f"requested={commit}; resolved={resolved}",
        )
    return resolved


def _is_ancestor(repository: Path, ancestor: str, descendant: str) -> bool:
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=repository,
        check=False,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        return False
    detail = completed.stderr.decode("utf-8", errors="replace").strip()
    raise IntegrationRevalidationError("GIT_ANCESTRY_FAILED", detail)


def _collect_delta_entries(
    repository: Path,
    base: str,
    head: str,
    *,
    exclusions: Sequence[str],
) -> tuple[GitDeltaEntry, ...]:
    args = [
        "git",
        "-c",
        "core.quotepath=false",
        "diff",
        "--name-status",
        "-z",
        "--find-renames",
        "--find-copies",
        f"{base}..{head}",
        "--",
        ".",
        *(f":(exclude,literal){path}" for path in exclusions),
    ]
    completed = subprocess.run(
        args,
        cwd=repository,
        check=False,
        capture_output=True,
        timeout=60,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise IntegrationRevalidationError("GIT_DIFF_FAILED", detail)
    tokens = completed.stdout.split(b"\0")
    entries: list[GitDeltaEntry] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        index += 1
        if not token:
            continue
        status = token.decode("ascii", errors="strict")
        path_count = 2 if status.startswith(("R", "C")) else 1
        if index + path_count > len(tokens):
            raise IntegrationRevalidationError("GIT_DIFF_FORMAT", status)
        paths = tuple(
            _portable_path(tokens[index + offset].decode("utf-8", errors="strict"))
            for offset in range(path_count)
        )
        index += path_count
        entries.append(GitDeltaEntry(status=status, paths=paths))
    return tuple(sorted(entries, key=lambda entry: (entry.paths, entry.status)))


def _classify_overlaps(
    *,
    task_paths: Sequence[str],
    mainline_paths: Sequence[str],
    declared_shared_paths: Sequence[str],
    policy: IntegrationRevalidationPolicy,
) -> tuple[PathOverlap, ...]:
    overlaps: list[PathOverlap] = []
    for task_path in task_paths:
        for mainline_path in mainline_paths:
            if not _paths_overlap(task_path, mainline_path):
                continue
            if _covered_by_scope(
                task_path,
                policy.contract_sensitive_scopes,
            ) or _covered_by_scope(mainline_path, policy.contract_sensitive_scopes):
                classification = "CONTRACT_SENSITIVE_OVERLAP"
                reason = "reviewed contract-sensitive scope changed on both histories"
            elif (
                _covered_by_scope(task_path, policy.coordinator_refreshable_scopes)
                and _covered_by_scope(
                    mainline_path,
                    policy.coordinator_refreshable_scopes,
                )
                and _covered_by_scope(task_path, declared_shared_paths)
            ):
                classification = "COORDINATOR_REFRESH"
                reason = "discard lane bytes and rebuild reviewed shared view on final tree"
            else:
                classification = "DOMAIN_OVERLAP"
                reason = "same domain scope changed on lane and mainline"
            overlaps.append(
                PathOverlap(
                    task_path=task_path,
                    mainline_path=mainline_path,
                    classification=classification,
                    reason=reason,
                )
            )
    return tuple(sorted(set(overlaps)))


def _contract_conflicts(
    task_claims: Sequence[ContractClaim],
    mainline_claims: Sequence[ContractClaim],
) -> tuple[dict[str, str], ...]:
    task_by_id = {claim.contract_id: claim for claim in task_claims}
    main_by_id = {claim.contract_id: claim for claim in mainline_claims}
    conflicts: list[dict[str, str]] = []
    for contract_id in sorted(set(task_by_id) & set(main_by_id)):
        task = task_by_id[contract_id]
        main = main_by_id[contract_id]
        if task.version != main.version:
            code = "CONTRACT_VERSION_CONFLICT"
        elif ContractAccess.WRITE in {task.access, main.access}:
            code = "CONTRACT_ACCESS_CONFLICT"
        else:
            continue
        conflicts.append(
            {
                "code": code,
                "contract_id": contract_id,
                "task_version": task.version,
                "mainline_version": main.version,
                "task_access": task.access.value,
                "mainline_access": main.access.value,
            }
        )
    return tuple(conflicts)


def _classify_all_paths(
    *,
    task_paths: Sequence[str],
    mainline_paths: Sequence[str],
    overlaps: Sequence[PathOverlap],
) -> tuple[dict[str, str], ...]:
    rows: list[dict[str, str]] = []
    overlapping_task = {row.task_path for row in overlaps}
    overlapping_mainline = {row.mainline_path for row in overlaps}
    for path in task_paths:
        if path not in overlapping_task:
            rows.append(
                {
                    "history": "TASK",
                    "path": path,
                    "classification": "TASK_ONLY",
                    "reason": "changed only on frozen task lane",
                }
            )
    for path in mainline_paths:
        if path not in overlapping_mainline:
            rows.append(
                {
                    "history": "MAINLINE",
                    "path": path,
                    "classification": "MAINLINE_UNRELATED",
                    "reason": "latest-main drift does not overlap task delta",
                }
            )
    for overlap in overlaps:
        rows.append(
            {
                "history": "BOTH",
                "path": f"{overlap.task_path}<->{overlap.mainline_path}",
                "classification": overlap.classification,
                "reason": overlap.reason,
            }
        )
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                row["history"],
                row["path"].casefold(),
                row["classification"],
            ),
        )
    )


def _parse_contract_claims(
    values: Sequence[ContractClaim | Mapping[str, Any]],
) -> tuple[ContractClaim, ...]:
    claims: list[ContractClaim] = []
    for value in values:
        if isinstance(value, ContractClaim):
            claim = value
        else:
            _require_exact_keys(
                value,
                {"contract_id", "version", "access"},
                "MAINLINE_CONTRACT_CLAIM_FIELDS",
            )
            try:
                access = ContractAccess(str(value["access"]))
            except ValueError as exc:
                raise IntegrationRevalidationError(
                    "MAINLINE_CONTRACT_ACCESS",
                    str(value["access"]),
                ) from exc
            claim = ContractClaim(
                contract_id=_identifier(value["contract_id"], "contract_id"),
                version=_identifier(value["version"], "version"),
                access=access,
            )
        claims.append(claim)
    ordered = tuple(sorted(claims))
    if len({claim.contract_id for claim in ordered}) != len(ordered):
        raise IntegrationRevalidationError(
            "MAINLINE_CONTRACT_DUPLICATE",
            "contract_id must be unique",
        )
    return ordered


def _contract_claim_dict(claim: ContractClaim) -> dict[str, str]:
    return {
        "contract_id": claim.contract_id,
        "version": claim.version,
        "access": claim.access.value,
    }


def _entry_paths(entries: Sequence[GitDeltaEntry]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {path for entry in entries for path in entry.paths},
            key=lambda value: value.casefold(),
        )
    )


def _covered_by_scope(path: str, scopes: Sequence[str]) -> bool:
    return any(path == scope or path.startswith(f"{scope}/") for scope in scopes)


def _paths_overlap(left: str, right: str) -> bool:
    return left == right or left.startswith(f"{right}/") or right.startswith(f"{left}/")


def _next_stage(decision: str) -> str:
    return {
        "READY_FOR_SINGLE_INTEGRATION_CANDIDATE": "COORDINATOR_INTEGRATION",
        "RECONCILIATION_REQUIRED": "COORDINATOR_RECONCILIATION",
        "SERIAL_CONTRACT_WAVE_REQUIRED": "SERIAL_CONTRACT_WAVE",
        "BLOCKED": "RESOLVE_BLOCKERS",
    }[decision]


def _issue(code: str, detail: str) -> dict[str, str]:
    return {"code": code, "detail": detail}


def _run_git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise IntegrationRevalidationError(
            "GIT_COMMAND_FAILED",
            f"git {' '.join(arguments)}: {detail}",
        )
    return completed.stdout.strip()


def _portable_path(value: object) -> str:
    if not isinstance(value, str):
        raise IntegrationRevalidationError("PATH_TYPE", "path must be a string")
    text = value.strip().replace("\\", "/")
    candidate = PurePosixPath(text)
    if (
        not text
        or candidate.is_absolute()
        or re.match(r"^[A-Za-z]:/", text)
        or ".." in candidate.parts
        or text.startswith("./")
    ):
        raise IntegrationRevalidationError("PATH_UNSAFE", text)
    normalized = candidate.as_posix()
    if normalized in {"", "."}:
        raise IntegrationRevalidationError("PATH_EMPTY", text)
    return normalized


def _unique_paths(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise IntegrationRevalidationError("POLICY_LIST", f"{field} must be a list")
    paths = tuple(_portable_path(item) for item in value)
    if len(paths) != len(set(paths)):
        raise IntegrationRevalidationError("POLICY_DUPLICATE", field)
    return tuple(sorted(paths, key=lambda item: item.casefold()))


def _unique_strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise IntegrationRevalidationError("POLICY_LIST", f"{field} must be a non-empty list")
    items = tuple(_identifier(item, field) for item in value)
    if len(items) != len(set(items)):
        raise IntegrationRevalidationError("POLICY_DUPLICATE", field)
    return tuple(sorted(items))


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}",
        value,
    ):
        raise IntegrationRevalidationError("IDENTIFIER", field)
    return value


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IntegrationRevalidationError("TEXT", field)
    return value.strip()


def _commit(value: object, field: str) -> str:
    text = _required_text(value, field)
    if not _COMMIT_RE.fullmatch(text):
        raise IntegrationRevalidationError("COMMIT", field)
    return text


def _sha256(value: object, field: str) -> str:
    text = _required_text(value, field)
    if not _SHA256_RE.fullmatch(text):
        raise IntegrationRevalidationError("SHA256", field)
    return text


def _require_exact_keys(
    value: Mapping[str, Any],
    expected: set[str],
    code: str,
) -> None:
    actual = set(value)
    if actual != expected:
        raise IntegrationRevalidationError(
            code,
            f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}",
        )


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
