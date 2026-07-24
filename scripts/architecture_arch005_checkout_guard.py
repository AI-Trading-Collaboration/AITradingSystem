from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardError,
    CheckoutLeaseGuard,
    CheckoutOperationClass,
)
from ai_trading_system.platform.architecture.parallel_control import ParallelControlError

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ARCH-005S4D checkout-scoped write and daily operation guard"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    acquire = subparsers.add_parser("acquire")
    acquire.add_argument("--intent-id", required=True)
    acquire.add_argument("--task-id", required=True)
    acquire.add_argument("--thread-id", required=True)
    acquire.add_argument("--actor", default="architecture-control-plane")
    acquire.add_argument(
        "--operation-class",
        choices=(
            CheckoutOperationClass.DOMAIN_MUTATION.value,
            CheckoutOperationClass.SHARED_MUTATION.value,
            CheckoutOperationClass.READ_ONLY_AUDIT.value,
        ),
        required=True,
    )
    acquire.add_argument("--base-commit")
    acquire.add_argument("--owned-path", action="append", default=[])
    acquire.add_argument("--shared-path", action="append", default=[])
    acquire.add_argument("--at")

    heartbeat = subparsers.add_parser("heartbeat")
    heartbeat.add_argument("--lease-id", required=True)
    heartbeat.add_argument("--actor", required=True)
    heartbeat.add_argument("--at")

    release = subparsers.add_parser("release")
    release.add_argument("--lease-id", required=True)
    release.add_argument("--actor", required=True)
    release.add_argument("--outcome", default="completed")
    release.add_argument("--evidence-ref", action="append", default=[])
    release.add_argument("--at")

    subparsers.add_parser("replay")

    daily = subparsers.add_parser("daily-preflight")
    daily.add_argument("--intent-id", required=True)
    daily.add_argument("--thread-id", default="manual-daily-preflight")
    daily.add_argument("--at")

    args = parser.parse_args()
    guard = CheckoutLeaseGuard(project_root=PROJECT_ROOT)
    try:
        if args.command == "acquire":
            decision, _ = guard.acquire(
                intent_id=args.intent_id,
                task_id=args.task_id,
                thread_id=args.thread_id,
                actor=args.actor,
                operation_class=CheckoutOperationClass(args.operation_class),
                owned_paths=tuple(args.owned_path),
                shared_paths=tuple(args.shared_path),
                base_commit=args.base_commit,
                now=_parse_datetime(args.at),
            )
            payload = decision.to_dict()
        elif args.command == "heartbeat":
            lease = guard.store.heartbeat(
                args.lease_id,
                actor=args.actor,
                now=_parse_datetime(args.at) or datetime.now(tz=UTC),
            )
            payload = {
                "status": "PASS",
                "action": "heartbeat",
                "lease": lease.to_dict(),
                "production_effect": "none",
            }
        elif args.command == "release":
            lease = guard.store.release(
                args.lease_id,
                actor=args.actor,
                now=_parse_datetime(args.at) or datetime.now(tz=UTC),
                evidence_refs=tuple(args.evidence_ref),
                reason_codes=(f"CHECKOUT_OPERATION_{args.outcome.upper()}",),
            )
            payload = {
                "status": "PASS",
                "action": "release",
                "lease": lease.to_dict(),
                "production_effect": "none",
            }
        elif args.command == "daily-preflight":
            decision, handle = guard.acquire(
                intent_id=args.intent_id,
                task_id="OPS-DAILY-UNIFIED-TRIGGER",
                thread_id=args.thread_id,
                actor="operations-automation",
                operation_class=CheckoutOperationClass.DAILY_OPERATION,
                now=_parse_datetime(args.at),
            )
            payload = decision.to_dict()
            if handle is not None:
                handle.release(outcome="preflight_only")
        else:
            payload = guard.replay().to_dict()
    except (CheckoutGuardError, ParallelControlError) as exc:
        payload = {
            "status": "BLOCKED",
            "error_code": exc.code,
            "message": exc.message,
            "production_effect": "none",
            "broker_action": "none",
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") == "PASS" else 1


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--at must include a timezone")
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
