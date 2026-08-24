from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.platform.architecture.task_registry_canonical import (
    build_cutover_candidate,
    refresh_consumer_inventory,
    register_task,
    run_rollback_rehearsal,
    update_task,
    validate_canonical_registry,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ARCH-005 S5 canonical task-registry authority",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="perform the one-time final legacy import")
    build.add_argument("--source-commit", required=True)
    _add_publication_argument(build)

    subparsers.add_parser("validate", help="validate canonical authority and generated views")
    refresh = subparsers.add_parser(
        "refresh-consumers",
        help="refresh the typed consumer inventory after an audited consumer migration",
    )
    _add_publication_argument(refresh)

    update = subparsers.add_parser("update", help="append one governed task-state event")
    _add_event_arguments(update)
    update.add_argument("--task-id", required=True)
    update.add_argument("--status")
    update.add_argument("--next-owner")
    update.add_argument("--blocker-or-next-step")
    update.add_argument("--acceptance-criteria")
    update.add_argument("--notes")
    _add_publication_argument(update)

    register = subparsers.add_parser("register", help="register one new canonical task")
    _add_event_arguments(register)
    register.add_argument(
        "--cells-json",
        required=True,
        help="JSON array containing the exact eight compatibility-view cells",
    )
    _add_publication_argument(register)

    rollback = subparsers.add_parser(
        "rollback-rehearsal",
        help="render owner-review legacy-compatible snapshots without changing authority",
    )
    rollback.add_argument("--output-root", required=True, type=Path)

    args = parser.parse_args()
    command = str(args.command)
    _require_publication_transaction(args)
    if command == "build":
        payload = build_cutover_candidate(
            project_root=PROJECT_ROOT,
            source_commit=str(args.source_commit),
        )
    elif command == "validate":
        registry = validate_canonical_registry(project_root=PROJECT_ROOT)
        payload = {
            "status": "PASS",
            "source_of_truth": registry.index["source_of_truth"],
            "task_count": registry.index["task_count"],
            "active_task_count": registry.index["active_task_count"],
            "completed_task_count": registry.index["completed_task_count"],
            "governance_cycle_count": registry.index["governance_cycle_count"],
            "manual_row_move_workflow_enabled": registry.index["manual_row_move_workflow_enabled"],
            "production_effect": "none",
            "broker_action": "none",
        }
    elif command == "refresh-consumers":
        payload = refresh_consumer_inventory(project_root=PROJECT_ROOT)
    elif command == "update":
        payload = update_task(
            project_root=PROJECT_ROOT,
            task_id=str(args.task_id),
            actor=str(args.actor),
            change_id=str(args.change_id),
            occurred_at=str(args.occurred_at),
            base_commit=str(args.base_commit),
            status=args.status,
            next_owner=args.next_owner,
            blocker_or_next_step=args.blocker_or_next_step,
            acceptance_criteria=args.acceptance_criteria,
            notes=args.notes,
        )
    elif command == "register":
        payload = register_task(
            project_root=PROJECT_ROOT,
            cells=_cells(str(args.cells_json)),
            actor=str(args.actor),
            change_id=str(args.change_id),
            occurred_at=str(args.occurred_at),
            base_commit=str(args.base_commit),
        )
    else:
        payload = run_rollback_rehearsal(
            project_root=PROJECT_ROOT,
            output_root=args.output_root,
        )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _add_event_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--actor", required=True)
    parser.add_argument("--change-id", required=True)
    parser.add_argument("--occurred-at", required=True)
    parser.add_argument("--base-commit", required=True)


def _add_publication_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--publication-transaction",
        required=True,
        type=Path,
        help=("Active integration_publication_fence.v1 transaction at TASK_SOURCE_PRE_WRITE."),
    )


def _require_publication_transaction(args: argparse.Namespace) -> None:
    if args.command not in {"build", "refresh-consumers", "update", "register"}:
        return
    task_id: str | None = None
    if args.command == "update":
        task_id = str(args.task_id)
    elif args.command == "register":
        cells = _cells(str(args.cells_json))
        if not cells:
            raise SystemExit("--cells-json must contain a task id")
        task_id = cells[0]
    fence = IntegrationPublicationFence(project_root=PROJECT_ROOT)
    try:
        fence.validate(
            args.publication_transaction,
            exact_phase="TASK_SOURCE_PRE_WRITE",
            task_id=task_id,
        )
    except PublicationFenceError as exc:
        raise SystemExit(f"publication transaction rejected task-source mutation: {exc}") from exc


def _cells(raw: str) -> list[str]:
    try:
        value: Any = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--cells-json is not valid JSON: {exc}") from exc
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise SystemExit("--cells-json must be a JSON array of strings")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
