#!/usr/bin/env python3
"""Read-only governed development preflight for AITradingSystem."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Any

SCHEMA_VERSION = "governed_development_preflight.v1"
MODES = ("READ_ONLY", "SINGLE_LANE", "DUAL_LANE")
STAGES = ("START", "LANE", "INTEGRATION", "CLOSEOUT")
ROLES = ("reader", "worker", "coordinator")
DUAL_LANES = {"engineering", "strategy-evidence"}

COORDINATOR_EXACT_PATHS = {
    "AGENTS.md",
    "config/report_registry.yaml",
    "docs/artifact_catalog.md",
    "docs/system_flow.md",
    "docs/task_register.md",
    "docs/task_register_completed.md",
}
COORDINATOR_PREFIXES = (
    "inputs/architecture",
    "registry/development_tasks_shadow",
)


class PreflightError(RuntimeError):
    """Raised when a required read-only probe cannot complete."""


def _run(command: list[str], cwd: Path, *, timeout: int = 90) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise PreflightError(f"{' '.join(command)} failed: {detail}")
    return completed.stdout


def _run_json(command: list[str], cwd: Path) -> dict[str, Any]:
    raw = _run(command, cwd)
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PreflightError(f"{' '.join(command)} returned non-JSON output") from exc
    if not isinstance(value, dict):
        raise PreflightError(f"{' '.join(command)} returned non-object JSON")
    return value


def normalize_repo_path(raw: str) -> str:
    text = raw.strip().replace("\\", "/")
    candidate = PurePosixPath(text)
    if (
        not text
        or candidate.is_absolute()
        or re.match(r"^[A-Za-z]:/", text)
        or ".." in candidate.parts
    ):
        raise ValueError(f"unsafe repository path: {raw}")
    normalized = candidate.as_posix()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    if not normalized or normalized == ".":
        raise ValueError(f"empty repository path: {raw}")
    return normalized


def paths_overlap(left: str, right: str) -> bool:
    return left == right or left.startswith(f"{right}/") or right.startswith(f"{left}/")


def is_coordinator_only(path: str) -> bool:
    if path in COORDINATOR_EXACT_PATHS:
        return True
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in COORDINATOR_PREFIXES)


def parse_claims(values: list[str]) -> dict[str, list[str]]:
    claims: dict[str, list[str]] = {}
    for value in values:
        lane, separator, raw_path = value.partition("=")
        lane_id = lane.strip()
        if not separator or not lane_id:
            raise ValueError(f"claim must use lane=path: {value}")
        path = normalize_repo_path(raw_path)
        claims.setdefault(lane_id, []).append(path)
    return claims


def evaluate_claims(
    *,
    mode: str,
    role: str,
    claims: dict[str, list[str]],
    coordinator_paths: list[str],
    contract_change: bool,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    blockers: list[dict[str, str]] = []
    serial: list[dict[str, str]] = []
    normalized_coordinator = [normalize_repo_path(path) for path in coordinator_paths]

    for lane_id, paths in claims.items():
        if len(paths) != len(set(paths)):
            blockers.append(
                {
                    "code": "DUPLICATE_LANE_PATH",
                    "detail": lane_id,
                }
            )
        for path in paths:
            if is_coordinator_only(path):
                blockers.append(
                    {
                        "code": "COORDINATOR_ONLY_PATH_CLAIMED_BY_LANE",
                        "detail": f"{lane_id}:{path}",
                    }
                )
            for coordinator_path in normalized_coordinator:
                if paths_overlap(path, coordinator_path):
                    blockers.append(
                        {
                            "code": "LANE_COORDINATOR_PATH_CONFLICT",
                            "detail": f"{lane_id}:{path}<->{coordinator_path}",
                        }
                    )

    lane_ids = sorted(claims)
    for index, left_lane in enumerate(lane_ids):
        for right_lane in lane_ids[index + 1 :]:
            for left_path in claims[left_lane]:
                for right_path in claims[right_lane]:
                    if paths_overlap(left_path, right_path):
                        blockers.append(
                            {
                                "code": "LANE_PATH_CONFLICT",
                                "detail": (f"{left_lane}:{left_path}<->{right_lane}:{right_path}"),
                            }
                        )

    if mode == "READ_ONLY" and (claims or normalized_coordinator):
        blockers.append(
            {
                "code": "READ_ONLY_WRITE_CLAIMS_FORBIDDEN",
                "detail": "READ_ONLY mode cannot declare write paths",
            }
        )
    if mode == "SINGLE_LANE" and not claims and not normalized_coordinator:
        blockers.append(
            {
                "code": "SINGLE_LANE_SCOPE_REQUIRED",
                "detail": "declare at least one lane or coordinator path",
            }
        )
    if mode == "DUAL_LANE":
        if set(claims) != DUAL_LANES:
            blockers.append(
                {
                    "code": "DUAL_LANE_IDS_REQUIRED",
                    "detail": ",".join(sorted(set(claims))),
                }
            )
        for lane_id in DUAL_LANES:
            if not claims.get(lane_id):
                blockers.append(
                    {
                        "code": "DUAL_LANE_SCOPE_REQUIRED",
                        "detail": lane_id,
                    }
                )
        if role != "coordinator":
            blockers.append(
                {
                    "code": "DUAL_LANE_COORDINATOR_REQUIRED",
                    "detail": role,
                }
            )
        if contract_change:
            serial.append(
                {
                    "code": "SERIAL_CONTRACT_WAVE_REQUIRED",
                    "detail": "consumer-visible contract or policy change declared",
                }
            )
    return blockers, serial


def evaluate_checkout_remote_gate(
    *,
    mode: str,
    role: str,
    stage: str,
    remote_action: bool,
    current_branch: str,
    audit_status: object,
    dirty_paths: object,
    origin_main: object,
    origin_main_vs_local_main: object,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Evaluate branch/stage and read-only remote publication invariants."""

    blockers: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    on_main = current_branch == "main"

    if mode != "READ_ONLY" and on_main and stage in {"LANE", "INTEGRATION"}:
        blockers.append(
            {
                "code": "MUTATION_STAGE_ON_MAIN",
                "detail": stage,
            }
        )

    if mode != "READ_ONLY" and on_main and stage == "CLOSEOUT":
        if role != "coordinator":
            blockers.append(
                {
                    "code": "REMOTE_ACTION_REQUIRES_COORDINATOR",
                    "detail": role,
                }
            )
        if not remote_action:
            blockers.append(
                {
                    "code": "MAIN_CLOSEOUT_REQUIRES_REMOTE_ACTION",
                    "detail": stage,
                }
            )

    if remote_action:
        if mode == "READ_ONLY":
            blockers.append(
                {
                    "code": "REMOTE_ACTION_REQUIRES_GOVERNED_MODE",
                    "detail": mode,
                }
            )
        if stage != "CLOSEOUT":
            blockers.append(
                {
                    "code": "REMOTE_ACTION_REQUIRES_CLOSEOUT_STAGE",
                    "detail": stage,
                }
            )
        if not on_main:
            blockers.append(
                {
                    "code": "REMOTE_ACTION_REQUIRES_MAIN",
                    "detail": current_branch or "<detached>",
                }
            )
        if role != "coordinator" and not (mode != "READ_ONLY" and on_main and stage == "CLOSEOUT"):
            blockers.append(
                {
                    "code": "REMOTE_ACTION_REQUIRES_COORDINATOR",
                    "detail": role,
                }
            )
        if audit_status == "PASS":
            if not isinstance(dirty_paths, list):
                blockers.append(
                    {
                        "code": "REMOTE_ACTION_DIRTY_INVENTORY_UNREADABLE",
                        "detail": type(dirty_paths).__name__,
                    }
                )
            elif dirty_paths:
                blockers.append(
                    {
                        "code": "REMOTE_ACTION_DIRTY_WORKTREE",
                        "detail": ",".join(str(path) for path in dirty_paths),
                    }
                )
        if origin_main is None:
            blockers.append(
                {
                    "code": "REMOTE_MAIN_UNAVAILABLE",
                    "detail": "origin/main",
                }
            )
        elif not isinstance(origin_main_vs_local_main, dict):
            blockers.append(
                {
                    "code": "REMOTE_MAIN_ANCESTRY_UNAVAILABLE",
                    "detail": type(origin_main_vs_local_main).__name__,
                }
            )
        else:
            origin_only = origin_main_vs_local_main.get("origin_only")
            local_only = origin_main_vs_local_main.get("local_only")
            if not isinstance(origin_only, int) or not isinstance(local_only, int):
                blockers.append(
                    {
                        "code": "REMOTE_MAIN_ANCESTRY_UNAVAILABLE",
                        "detail": json.dumps(
                            origin_main_vs_local_main,
                            sort_keys=True,
                        ),
                    }
                )
            elif origin_only:
                blockers.append(
                    {
                        "code": "REMOTE_MAIN_NOT_CANDIDATE_ANCESTOR",
                        "detail": json.dumps(
                            origin_main_vs_local_main,
                            sort_keys=True,
                        ),
                    }
                )
    elif isinstance(origin_main_vs_local_main, dict):
        origin_only = origin_main_vs_local_main.get("origin_only")
        local_only = origin_main_vs_local_main.get("local_only")
        if (
            isinstance(origin_only, int)
            and isinstance(local_only, int)
            and (origin_only or local_only)
        ):
            warnings.append(
                {
                    "code": "REMOTE_DIVERGENCE_DISCLOSED_LOCAL_ONLY",
                    "detail": json.dumps(
                        origin_main_vs_local_main,
                        sort_keys=True,
                    ),
                }
            )

    return blockers, warnings


def evaluate_task_registration(
    *,
    mode: str,
    stage: str,
    task_id: str | None,
    active_task_register: str,
    completed_task_register: str,
) -> tuple[bool, str]:
    if mode == "READ_ONLY":
        return True, "READ_ONLY"
    active_registered = bool(task_id and task_id in active_task_register)
    completed_registered = bool(task_id and task_id in completed_task_register)
    if active_registered:
        return True, "ACTIVE"
    if stage == "CLOSEOUT" and completed_registered:
        return True, "COMPLETED_CLOSEOUT_ONLY"
    return False, "NONE"


def evaluate_base_drift(
    *,
    stage: str,
    current_branch: str,
    expected_base: str | None,
    local_main: str,
    head: str,
    expected_base_is_head_ancestor: bool | None,
    integration_plan: dict[str, Any] | None,
    reviewed_reconciliation_plan_id: str | None,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    blockers: list[dict[str, str]] = []
    serial: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    if expected_base is None or expected_base == local_main:
        if reviewed_reconciliation_plan_id is not None:
            blockers.append(
                {
                    "code": "UNEXPECTED_RECONCILIATION_APPROVAL",
                    "detail": reviewed_reconciliation_plan_id,
                }
            )
        return blockers, serial, warnings
    if (
        stage == "LANE"
        and current_branch != "main"
        and expected_base_is_head_ancestor is True
    ):
        warnings.append(
            {
                "code": "BASE_DRIFT_DEFERRED_TO_INTEGRATION_PLAN",
                "detail": f"{expected_base}!={local_main}",
            }
        )
        return blockers, serial, warnings
    if integration_plan is None:
        blockers.append(
            {
                "code": "EXPECTED_BASE_MISMATCH",
                "detail": f"{expected_base}!={local_main}",
            }
        )
        return blockers, serial, warnings
    expected_bindings = {
        "frozen_base": expected_base,
        "lane_head": head,
        "latest_main": local_main,
    }
    for field, expected in expected_bindings.items():
        if integration_plan.get(field) != expected:
            blockers.append(
                {
                    "code": "INTEGRATION_REVALIDATION_BINDING_MISMATCH",
                    "detail": (
                        f"{field}:{integration_plan.get(field)!r}!={expected!r}"
                    ),
                }
            )
    decision = integration_plan.get("decision")
    if decision == "READY_FOR_SINGLE_INTEGRATION_CANDIDATE":
        if reviewed_reconciliation_plan_id is not None:
            blockers.append(
                {
                    "code": "UNEXPECTED_RECONCILIATION_APPROVAL",
                    "detail": reviewed_reconciliation_plan_id,
                }
            )
        if stage != "INTEGRATION":
            blockers.append(
                {
                    "code": "INTEGRATION_REVALIDATION_STAGE_MISMATCH",
                    "detail": stage,
                }
            )
        elif integration_plan.get("candidate_creation_allowed") is not True:
            blockers.append(
                {
                    "code": "INTEGRATION_CANDIDATE_NOT_ALLOWED",
                    "detail": str(integration_plan.get("candidate_creation_allowed")),
                }
            )
    elif decision == "SERIAL_CONTRACT_WAVE_REQUIRED":
        serial.append(
            {
                "code": "SERIAL_CONTRACT_WAVE_REQUIRED",
                "detail": str(integration_plan.get("plan_id", "<missing>")),
            }
        )
    elif decision == "RECONCILIATION_REQUIRED":
        plan_id = str(integration_plan.get("plan_id", "<missing>"))
        if (
            stage == "INTEGRATION"
            and reviewed_reconciliation_plan_id == plan_id
            and integration_plan.get("reviewed_reconciliation_required") is True
        ):
            warnings.append(
                {
                    "code": "REVIEWED_BASE_DRIFT_RECONCILIATION",
                    "detail": plan_id,
                }
            )
        else:
            blockers.append(
                {
                    "code": "BASE_DRIFT_RECONCILIATION_REQUIRED",
                    "detail": plan_id,
                }
            )
    else:
        blockers.append(
            {
                "code": "INTEGRATION_REVALIDATION_NOT_READY",
                "detail": str(decision),
            }
        )
    return blockers, serial, warnings


def load_validated_integration_plan(
    *,
    repo: Path,
    plan_argument: str | None,
    manifest_argument: str | None,
) -> dict[str, Any] | None:
    if plan_argument is None and manifest_argument is None:
        return None
    if plan_argument is None or manifest_argument is None:
        raise PreflightError(
            "--integration-revalidation-plan and --change-manifest are required together"
        )
    plan_path = _resolve_repo_file(repo, plan_argument, "integration revalidation plan")
    manifest_path = _resolve_repo_file(repo, manifest_argument, "change manifest")
    validator = repo / "scripts" / "architecture_arch005_integration_revalidation.py"
    if not validator.is_file():
        raise PreflightError(f"integration revalidation validator missing: {validator}")
    _run(
        [
            sys.executable,
            str(validator),
            "validate",
            "--repository",
            str(repo),
            "--manifest",
            str(manifest_path),
            "--plan",
            str(plan_path),
        ],
        repo,
    )
    try:
        payload = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PreflightError(f"integration revalidation plan is unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise PreflightError("integration revalidation plan must be a JSON object")
    return payload


def _resolve_repo_file(repo: Path, raw: str, field: str) -> Path:
    candidate = Path(raw)
    resolved = (repo / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    try:
        resolved.relative_to(repo)
    except ValueError as exc:
        raise PreflightError(f"{field} escapes repository: {raw}") from exc
    if not resolved.is_file():
        raise PreflightError(f"{field} missing: {resolved}")
    return resolved


def _git_is_ancestor(repo: Path, ancestor: str, descendant: str) -> bool | None:
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        return False
    return None


def collect_repo_state(repo: Path) -> dict[str, Any]:
    guard = repo / "scripts" / "architecture_arch005_checkout_guard.py"
    if not guard.is_file():
        raise PreflightError(f"checkout guard missing: {guard}")
    audit = _run_json([sys.executable, str(guard), "worktree-audit"], repo)
    replay = _run_json([sys.executable, str(guard), "replay"], repo)
    current_branch = _run(["git", "branch", "--show-current"], repo).strip()
    local_main = _run(["git", "rev-parse", "main"], repo).strip()
    head = _run(["git", "rev-parse", "HEAD"], repo).strip()
    worktree_raw = _run(["git", "worktree", "list", "--porcelain"], repo)
    worktrees = [
        line.removeprefix("worktree ")
        for line in worktree_raw.splitlines()
        if line.startswith("worktree ")
    ]

    origin_main: str | None
    ahead_behind: dict[str, int] | None
    try:
        origin_main = _run(["git", "rev-parse", "origin/main"], repo).strip()
        raw_counts = _run(
            ["git", "rev-list", "--left-right", "--count", "origin/main...main"],
            repo,
        ).split()
        ahead_behind = {
            "origin_only": int(raw_counts[0]),
            "local_only": int(raw_counts[1]),
        }
    except (PreflightError, ValueError, IndexError):
        origin_main = None
        ahead_behind = None

    return {
        "current_branch": current_branch,
        "head": head,
        "local_main": local_main,
        "origin_main": origin_main,
        "origin_main_vs_local_main": ahead_behind,
        "worktrees": worktrees,
        "worktree_audit": audit,
        "lease_replay": replay,
    }


def build_result(args: argparse.Namespace) -> dict[str, Any]:
    repo = Path(args.repo).resolve()
    blockers: list[dict[str, str]] = []
    serial: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []

    if not (repo / "AGENTS.md").is_file() or not (repo / "docs" / "task_register.md").is_file():
        raise PreflightError(f"not an AITradingSystem repository root: {repo}")

    claims = parse_claims(args.claim)
    coordinator_paths = [normalize_repo_path(path) for path in args.coordinator_path]
    claim_blockers, claim_serial = evaluate_claims(
        mode=args.mode,
        role=args.role,
        claims=claims,
        coordinator_paths=coordinator_paths,
        contract_change=args.contract_change,
    )
    blockers.extend(claim_blockers)
    serial.extend(claim_serial)
    state = collect_repo_state(repo)
    integration_plan: dict[str, Any] | None = None
    try:
        integration_plan = load_validated_integration_plan(
            repo=repo,
            plan_argument=args.integration_revalidation_plan,
            manifest_argument=args.change_manifest,
        )
    except PreflightError as exc:
        blockers.append(
            {
                "code": "INTEGRATION_REVALIDATION_INVALID",
                "detail": str(exc),
            }
        )

    audit = state["worktree_audit"]
    if audit.get("status") != "PASS":
        blockers.append(
            {
                "code": "WORKTREE_AUDIT_NOT_PASS",
                "detail": str(audit.get("status")),
            }
        )
    replay = state["lease_replay"]
    if replay.get("status") != "PASS":
        blockers.append(
            {
                "code": "LEASE_REPLAY_NOT_PASS",
                "detail": str(replay.get("status")),
            }
        )
    active_leases = replay.get("active_leases")
    if not isinstance(active_leases, list):
        blockers.append(
            {
                "code": "ACTIVE_LEASES_UNREADABLE",
                "detail": type(active_leases).__name__,
            }
        )
        active_leases = []
    allowed_lease_ids = set(args.allow_active_lease)
    unexpected_active = [
        lease
        for lease in active_leases
        if not isinstance(lease, dict) or str(lease.get("lease_id")) not in allowed_lease_ids
    ]
    if args.mode != "READ_ONLY" and unexpected_active:
        blockers.append(
            {
                "code": "UNEXPECTED_ACTIVE_LEASE",
                "detail": ",".join(
                    (
                        str(lease.get("lease_id", "<unknown>"))
                        if isinstance(lease, dict)
                        else "<invalid>"
                    )
                    for lease in unexpected_active
                ),
            }
        )

    task_registered = args.mode == "READ_ONLY"
    task_registration_source = "READ_ONLY" if task_registered else "NONE"
    if args.mode != "READ_ONLY":
        active_task_register = (repo / "docs" / "task_register.md").read_text(
            encoding="utf-8"
        )
        completed_task_register_path = repo / "docs" / "task_register_completed.md"
        completed_task_register = (
            completed_task_register_path.read_text(encoding="utf-8")
            if completed_task_register_path.is_file()
            else ""
        )
        task_registered, task_registration_source = evaluate_task_registration(
            mode=args.mode,
            stage=args.stage,
            task_id=args.task_id,
            active_task_register=active_task_register,
            completed_task_register=completed_task_register,
        )
        if not task_registered:
            blockers.append(
                {
                    "code": "TASK_NOT_REGISTERED",
                    "detail": args.task_id or "<missing>",
                }
            )

    expected_base_is_head_ancestor = (
        _git_is_ancestor(repo, args.expected_base, state["head"])
        if args.expected_base
        else None
    )
    base_blockers, base_serial, base_warnings = evaluate_base_drift(
        stage=args.stage,
        current_branch=state["current_branch"],
        expected_base=args.expected_base,
        local_main=state["local_main"],
        head=state["head"],
        expected_base_is_head_ancestor=expected_base_is_head_ancestor,
        integration_plan=integration_plan,
        reviewed_reconciliation_plan_id=args.reviewed_reconciliation_plan_id,
    )
    blockers.extend(base_blockers)
    serial.extend(base_serial)
    warnings.extend(base_warnings)
    checkout_blockers, checkout_warnings = evaluate_checkout_remote_gate(
        mode=args.mode,
        role=args.role,
        stage=args.stage,
        remote_action=args.remote_action,
        current_branch=state["current_branch"],
        audit_status=audit.get("status"),
        dirty_paths=audit.get("dirty_paths"),
        origin_main=state["origin_main"],
        origin_main_vs_local_main=state["origin_main_vs_local_main"],
    )
    blockers.extend(checkout_blockers)
    warnings.extend(checkout_warnings)

    status = "BLOCKED" if blockers else "SERIAL_CONTRACT_WAVE_REQUIRED" if serial else "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "mode": args.mode,
        "role": args.role,
        "stage": args.stage,
        "task_id": args.task_id,
        "task_registered": task_registered,
        "task_registration_source": task_registration_source,
        "repository": repo.as_posix(),
        "git": {
            "current_branch": state["current_branch"],
            "head": state["head"],
            "local_main": state["local_main"],
            "origin_main": state["origin_main"],
            "origin_main_vs_local_main": state["origin_main_vs_local_main"],
            "expected_base": args.expected_base,
        },
        "worktree_audit": {
            "status": audit.get("status"),
            "dirty_paths": audit.get("dirty_paths"),
            "known_unrelated_exclusions": audit.get("known_unrelated_exclusions"),
        },
        "leases": {
            "status": replay.get("status"),
            "active_lease_ids": [
                lease.get("lease_id") for lease in active_leases if isinstance(lease, dict)
            ],
            "allowed_active_lease_ids": sorted(allowed_lease_ids),
        },
        "worktrees": state["worktrees"],
        "claims": claims,
        "coordinator_paths": coordinator_paths,
        "contract_change": args.contract_change,
        "integration_revalidation": (
            {
                "plan_id": integration_plan.get("plan_id"),
                "plan_sha256": integration_plan.get("plan_sha256"),
                "decision": integration_plan.get("decision"),
            }
            if integration_plan is not None
            else None
        ),
        "reviewed_reconciliation_plan_id": args.reviewed_reconciliation_plan_id,
        "remote_action_requested": args.remote_action,
        "blockers": blockers,
        "serial_requirements": serial,
        "warnings": warnings,
        "production_effect": "none",
        "broker_action": "none",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--role", choices=ROLES, default="reader")
    parser.add_argument("--stage", choices=STAGES, default="START")
    parser.add_argument("--task-id")
    parser.add_argument("--expected-base")
    parser.add_argument("--integration-revalidation-plan")
    parser.add_argument("--change-manifest")
    parser.add_argument("--reviewed-reconciliation-plan-id")
    parser.add_argument("--claim", action="append", default=[], help="lane=repo/path")
    parser.add_argument("--coordinator-path", action="append", default=[])
    parser.add_argument("--allow-active-lease", action="append", default=[])
    parser.add_argument("--contract-change", action="store_true")
    parser.add_argument("--remote-action", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = build_result(args)
    except (PreflightError, ValueError, subprocess.TimeoutExpired) as exc:
        result = {
            "schema_version": SCHEMA_VERSION,
            "status": "BLOCKED",
            "blockers": [
                {
                    "code": "PREFLIGHT_PROBE_FAILED",
                    "detail": str(exc),
                }
            ],
            "production_effect": "none",
            "broker_action": "none",
        }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    if result["status"] == "PASS":
        return 0
    if result["status"] == "SERIAL_CONTRACT_WAVE_REQUIRED":
        return 3
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
