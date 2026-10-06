from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.integration_publication_fence import (
    DEFAULT_POLICY_PATH,
    TASK_SOURCE_ONLY_KIND,
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.workflow_contract import WorkflowContractError
from ai_trading_system.platform.architecture.workflow_execution import (
    ExecutionContainmentError,
    acceptance_runtime_identity,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFILE_CHECKPOINT_PHASES = frozenset({"LOCAL_MAIN_FF_PRE", "REMOTE_PUSH_PRE"})
LOCAL_PUBLICATION_COMMANDS = frozenset({
    "local-publication-inspect", "local-publication-recover-index", "local-publication-hook",
    "local-publish", "local-publication-worker", "local-publication-adopt-published",
    "local-publication-recover",
})


def require_entry_loaded_source_custody(args: argparse.Namespace) -> None:
    """Profile-consuming entrypoints must prove their own loaded code first.

    The isolated (-I) Full profile inspector never loads this process's startup
    or PYTHONPATH code, so it cannot observe a changed caller implementation.
    The check is scoped to this process entry; library callers inside other
    processes (for example a test worker) are not attested by it.
    """
    if not (
        args.command in LOCAL_PUBLICATION_COMMANDS
        or (args.command == "checkpoint" and args.phase in PROFILE_CHECKPOINT_PHASES)
    ):
        return
    try:
        acceptance_runtime_identity()
    except ExecutionContainmentError as exc:
        raise PublicationFenceError("PUBLICATION_FULL_CLOSURE_INVALID", str(exc)) from exc


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
    acquire.add_argument(
        "--task-id-extra", action="append", default=[],
        help="another task whose row this transaction may write (repeatable; DEVX-016 S2)",
    )
    acquire.add_argument(
        "--task-source-only", action="store_true",
        help="a transaction that only registers/moves task rows and may complete after "
        "TASK_SOURCE_PRE_WRITE (it can never reach a candidate or publication phase)",
    )

    replay = subparsers.add_parser("replay")
    replay.add_argument("--transaction", required=True, type=Path)

    remote = subparsers.add_parser("remote-observe")
    remote.add_argument("--transaction", required=True, type=Path)

    local = subparsers.add_parser("local-publication-inspect")
    local.add_argument("--transaction", required=True, type=Path)

    recover_index = subparsers.add_parser("local-publication-recover-index")
    recover_index.add_argument("--transaction", required=True, type=Path)
    recover_index.add_argument("--actor", default="integration-coordinator")

    hook = subparsers.add_parser("local-publication-hook")
    hook.add_argument("--transaction", required=True, type=Path)
    hook.add_argument("--actor", required=True)
    hook.add_argument("--request-sha", required=True)
    hook.add_argument("--kind", required=True, choices=("reference-transaction", "post-merge"))
    hook.add_argument("--stage", required=True)

    publish = subparsers.add_parser("local-publish")
    publish.add_argument("--transaction", required=True, type=Path)
    publish.add_argument("--actor", default="integration-coordinator")
    publish.add_argument("--authorize-peer-head-handoff", action="store_true")

    worker = subparsers.add_parser("local-publication-worker")
    worker.add_argument("--transaction", required=True, type=Path)
    worker.add_argument("--actor", required=True)
    worker.add_argument("--request-id", required=True)
    worker.add_argument("--authorize-peer-head-handoff", action="store_true")

    recover_published = subparsers.add_parser("local-publication-adopt-published")
    recover_published.add_argument("--transaction", required=True, type=Path)
    recover_published.add_argument("--actor", default="integration-coordinator")

    recover_local = subparsers.add_parser("local-publication-recover")
    recover_local.add_argument("--transaction", required=True, type=Path)
    recover_local.add_argument("--actor", default="integration-coordinator")

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
        require_entry_loaded_source_custody(args)
        payload = _dispatch(fence, args)
    except (PublicationFenceError, ParallelControlError, WorkflowContractError,
            ExecutionContainmentError, OSError, ValueError) as exc:
        code = (exc.code if isinstance(
            exc, (PublicationFenceError, ParallelControlError, WorkflowContractError,
                  ExecutionContainmentError),
        ) else "PUBLICATION_COMMAND_FAILED")
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
    incomplete_publication = args.command in {
        "local-publish", "local-publication-adopt-published", "local-publication-recover",
    } and payload.get("status") in {"RECOVERY_REQUIRED", "OBSERVE_ONLY"}
    return 2 if payload.get("status") == "REMOTE_UNKNOWN" or incomplete_publication else 0


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
            task_ids=args.task_id_extra,
            kind=TASK_SOURCE_ONLY_KIND if args.task_source_only else None,
            now=datetime.now(tz=UTC),
        )
    if args.command == "replay":
        return fence.replay(args.transaction).to_dict()
    if args.command == "remote-observe":
        return fence.observe_remote(args.transaction)
    if args.command == "local-publication-inspect":
        return fence.inspect_local_publication(args.transaction)
    if args.command == "local-publication-hook":
        from ai_trading_system.platform.architecture.workflow_coordination import (
            PublicationLifecycle,
        )

        return PublicationLifecycle(fence).record_publication_hook(
            args.transaction, actor=args.actor, request_sha=args.request_sha,
            kind=args.kind, stage=args.stage, updates=sys.stdin.buffer.read(257),
        )
    if args.command in {
        "local-publish", "local-publication-worker", "local-publication-adopt-published",
        "local-publication-recover",
    }:
        from ai_trading_system.platform.architecture.workflow_coordination import (
            PublicationLifecycle,
        )

        lifecycle = PublicationLifecycle(fence)
        if args.command == "local-publication-recover":
            return lifecycle.recover_local_publication(args.transaction, actor=args.actor)
        if args.command == "local-publish":
            return lifecycle.publish_local(
                args.transaction, actor=args.actor,
                authorize_peer_head_handoff=args.authorize_peer_head_handoff,
            )
        if args.command == "local-publication-worker":
            return lifecycle.run_publication_worker(
                args.transaction, actor=args.actor, request_id=args.request_id,
                authorize_peer_head_handoff=args.authorize_peer_head_handoff,
            )
        replay = fence.replay(args.transaction)
        if replay.status != "PASS" or replay.transaction["actor"] != args.actor:
            raise PublicationFenceError("PUBLICATION_ACTOR_MISMATCH", args.actor)
        lease = str(replay.transaction["lease_id"])
        observed = lifecycle.recover(lease, actor=args.actor)
        if observed["status"] not in {"RECOVERED_TERMINAL", "REPLAY_ONLY"}:
            return {key: item for key, item in observed.items() if key != "execution"}
        return lifecycle.adopt_published_attempt(lease, actor=args.actor, recovery=True)
    if args.command == "local-publication-recover-index":
        from ai_trading_system.platform.architecture.workflow_coordination import (
            PublicationLifecycle,
        )

        fence.validate(args.transaction, exact_phase="LOCAL_MAIN_FF_PRE", require_candidate=True)
        replay = fence.replay(args.transaction)
        if replay.transaction.get("actor") != args.actor:
            raise PublicationFenceError("PUBLICATION_ACTOR_MISMATCH", args.actor)
        return PublicationLifecycle(fence).adopt_index_replaced_failed_attempt(
            str(replay.transaction["lease_id"]), actor=args.actor,
        )
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
