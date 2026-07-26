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
                    str(lease.get("lease_id", "<unknown>"))
                    if isinstance(lease, dict)
                    else "<invalid>"
                    for lease in unexpected_active
                ),
            }
        )

    task_registered = args.mode == "READ_ONLY"
    if args.mode != "READ_ONLY":
        task_register = (repo / "docs" / "task_register.md").read_text(encoding="utf-8")
        task_registered = bool(args.task_id and args.task_id in task_register)
        if not task_registered:
            blockers.append(
                {
                    "code": "TASK_NOT_REGISTERED",
                    "detail": args.task_id or "<missing>",
                }
            )

    if args.expected_base and args.expected_base != state["local_main"]:
        blockers.append(
            {
                "code": "EXPECTED_BASE_MISMATCH",
                "detail": f"{args.expected_base}!={state['local_main']}",
            }
        )
    if args.mode != "READ_ONLY" and args.stage != "START" and state["current_branch"] == "main":
        blockers.append(
            {
                "code": "MUTATION_STAGE_ON_MAIN",
                "detail": args.stage,
            }
        )
    if args.remote_action and state["origin_main"] is None:
        blockers.append(
            {
                "code": "REMOTE_MAIN_UNAVAILABLE",
                "detail": "origin/main",
            }
        )
    elif not args.remote_action and state["origin_main_vs_local_main"]:
        counts = state["origin_main_vs_local_main"]
        if counts["origin_only"] or counts["local_only"]:
            warnings.append(
                {
                    "code": "REMOTE_DIVERGENCE_DISCLOSED_LOCAL_ONLY",
                    "detail": json.dumps(counts, sort_keys=True),
                }
            )

    status = "BLOCKED" if blockers else "SERIAL_CONTRACT_WAVE_REQUIRED" if serial else "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "mode": args.mode,
        "role": args.role,
        "stage": args.stage,
        "task_id": args.task_id,
        "task_registered": task_registered,
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
