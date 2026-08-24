from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.integration_publication_fence import (
    DEFAULT_POLICY_PATH,
    IntegrationPublicationFence,
    PublicationFenceError,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ARCH-005 coordinator integration publication transaction",
    )
    parser.add_argument("--repository", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)

    acquire = subparsers.add_parser("acquire")
    acquire.add_argument("--transaction-id", required=True)
    acquire.add_argument("--task-id", required=True)
    acquire.add_argument("--change-id", required=True)
    acquire.add_argument("--thread-id", required=True)
    acquire.add_argument("--actor", default="integration-coordinator")
    acquire.add_argument("--frozen-base", required=True)
    acquire.add_argument("--lane-head", required=True)
    acquire.add_argument("--expected-main", required=True)
    acquire.add_argument("--owned-path", action="append", default=[])
    acquire.add_argument("--shared-path", action="append", default=[])
    acquire.add_argument("--generator-id", action="append", default=[])
    acquire.add_argument("--required-tier", action="append")
    acquire.add_argument("--integration-plan", type=Path)
    acquire.add_argument("--full-parent", type=Path)

    replay = subparsers.add_parser("replay")
    replay.add_argument("--transaction", required=True, type=Path)

    validate = subparsers.add_parser("validate")
    validate.add_argument("--transaction", required=True, type=Path)
    validate.add_argument("--minimum-phase")
    validate.add_argument("--exact-phase")
    validate.add_argument("--task-id")
    validate.add_argument("--validation-tier")
    validate.add_argument("--parent", type=Path)
    validate.add_argument("--require-candidate", action="store_true")

    checkpoint = subparsers.add_parser("checkpoint")
    checkpoint.add_argument("--transaction", required=True, type=Path)
    checkpoint.add_argument("--phase", required=True)
    checkpoint.add_argument("--actor", default="integration-coordinator")
    checkpoint.add_argument("--evidence", action="append", default=[], type=Path)
    checkpoint.add_argument("--generator-id", action="append", default=[])
    checkpoint.add_argument("--full-run-id")
    checkpoint.add_argument("--validation-status")

    release = subparsers.add_parser("release")
    release.add_argument("--transaction", required=True, type=Path)
    release.add_argument("--actor", default="integration-coordinator")
    release.add_argument("--outcome", choices=("completed", "failed"), required=True)
    release.add_argument("--evidence", action="append", default=[], type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repository = args.repository.resolve()
    policy = args.policy.resolve()
    fence = IntegrationPublicationFence(
        project_root=repository,
        policy_path=policy,
    )
    try:
        payload = _dispatch(fence, args)
    except (PublicationFenceError, OSError, ValueError, json.JSONDecodeError) as exc:
        code = exc.code if isinstance(exc, PublicationFenceError) else "PUBLICATION_COMMAND_FAILED"
        payload = {
            "schema_version": "integration_publication_fence_command_result.v1",
            "status": "BLOCKED",
            "reason_code": code,
            "detail": str(exc),
            "production_effect": "none",
            "broker_action": "none",
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _dispatch(
    fence: IntegrationPublicationFence,
    args: argparse.Namespace,
) -> dict[str, Any]:
    if args.command == "acquire":
        return fence.acquire(
            transaction_id=args.transaction_id,
            task_id=args.task_id,
            change_id=args.change_id,
            thread_id=args.thread_id,
            actor=args.actor,
            frozen_base_sha=args.frozen_base,
            lane_head_sha=args.lane_head,
            expected_main_sha=args.expected_main,
            owned_paths=args.owned_path,
            shared_paths=args.shared_path,
            generator_ids=args.generator_id,
            required_validation_tiers=args.required_tier,
            integration_plan_path=args.integration_plan,
            full_parent_path=args.full_parent,
            now=datetime.now(tz=UTC),
        )
    if args.command == "replay":
        return fence.replay(args.transaction).to_dict()
    if args.command == "validate":
        return fence.validate(
            args.transaction,
            minimum_phase=args.minimum_phase,
            exact_phase=args.exact_phase,
            task_id=args.task_id,
            validation_tier=args.validation_tier,
            parent_path=args.parent,
            require_candidate=args.require_candidate,
        )
    if args.command == "checkpoint":
        return fence.checkpoint(
            args.transaction,
            phase=args.phase,
            actor=args.actor,
            evidence_paths=args.evidence,
            generator_ids=args.generator_id,
            full_run_id=args.full_run_id,
            validation_status=args.validation_status,
        )
    if args.command == "release":
        return fence.release(
            args.transaction,
            actor=args.actor,
            outcome=args.outcome,
            evidence_paths=args.evidence,
        )
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
