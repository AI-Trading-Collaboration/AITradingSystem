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
from ai_trading_system.platform.architecture.checkout_telemetry import (
    build_checkout_telemetry_rollup,
    build_checkout_telemetry_snapshot,
    validate_checkout_telemetry_rollup,
    validate_checkout_telemetry_snapshot,
    write_checkout_telemetry_rollup,
    write_checkout_telemetry_snapshot,
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

    telemetry = subparsers.add_parser("telemetry-build")
    telemetry.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    telemetry.add_argument("--runtime-root", type=Path)
    telemetry.add_argument("--batch-id", required=True)
    telemetry.add_argument(
        "--batch-kind",
        choices=(
            "supervised_automation",
            "s4c_integration",
            "manual_control_plane",
        ),
        required=True,
    )
    telemetry.add_argument("--supervised-run", type=Path, action="append", default=[])
    telemetry.add_argument("--handoff", type=Path, action="append", default=[])
    telemetry.add_argument("--reconciliation", type=Path, action="append", default=[])
    telemetry.add_argument("--false-block-review", type=Path)
    telemetry.add_argument("--output", type=Path, required=True)
    telemetry.add_argument("--at")

    telemetry_validate = subparsers.add_parser("telemetry-validate")
    telemetry_validate.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    telemetry_validate.add_argument("--artifact", type=Path, required=True)

    rollup = subparsers.add_parser("telemetry-rollup")
    rollup.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    rollup.add_argument("--snapshot", type=Path, action="append", required=True)
    rollup.add_argument("--output", type=Path, required=True)
    rollup.add_argument("--at")

    rollup_validate = subparsers.add_parser("telemetry-rollup-validate")
    rollup_validate.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    rollup_validate.add_argument("--artifact", type=Path, required=True)

    args = parser.parse_args()
    try:
        if args.command == "telemetry-build":
            payload = build_checkout_telemetry_snapshot(
                project_root=args.project_root,
                runtime_root=args.runtime_root,
                batch_id=args.batch_id,
                batch_kind=args.batch_kind,
                supervised_run_paths=tuple(args.supervised_run),
                handoff_paths=tuple(args.handoff),
                reconciliation_paths=tuple(args.reconciliation),
                false_block_review_path=args.false_block_review,
                generated_at=_parse_datetime(args.at),
            )
            write_checkout_telemetry_snapshot(
                args.output,
                payload,
                project_root=args.project_root,
            )
        elif args.command == "telemetry-validate":
            telemetry_payload = _load_json_mapping(args.artifact)
            validate_checkout_telemetry_snapshot(
                telemetry_payload,
                project_root=args.project_root,
            )
            payload = {
                "status": "PASS",
                "artifact": str(args.artifact),
                "schema_version": telemetry_payload.get("schema_version"),
                "production_effect": "none",
            }
        elif args.command == "telemetry-rollup":
            payload = build_checkout_telemetry_rollup(
                project_root=args.project_root,
                snapshot_paths=tuple(args.snapshot),
                generated_at=_parse_datetime(args.at),
            )
            write_checkout_telemetry_rollup(
                args.output,
                payload,
                project_root=args.project_root,
            )
        elif args.command == "telemetry-rollup-validate":
            rollup_payload = _load_json_mapping(args.artifact)
            validate_checkout_telemetry_rollup(
                rollup_payload,
                project_root=args.project_root,
            )
            payload = {
                "status": "PASS",
                "artifact": str(args.artifact),
                "schema_version": rollup_payload.get("schema_version"),
                "production_effect": "none",
            }
        else:
            guard = CheckoutLeaseGuard(project_root=PROJECT_ROOT)
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
                lease = guard.release(
                    args.lease_id,
                    actor=args.actor,
                    now=_parse_datetime(args.at),
                    outcome=args.outcome,
                    evidence_refs=tuple(args.evidence_ref),
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
    except (CheckoutGuardError, ParallelControlError, OSError, json.JSONDecodeError) as exc:
        if isinstance(exc, (CheckoutGuardError, ParallelControlError)):
            code = exc.code
            message = exc.message
        else:
            code = type(exc).__name__
            message = str(exc)
        payload = {
            "status": "BLOCKED",
            "error_code": code,
            "message": message,
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


def _load_json_mapping(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CheckoutGuardError(
            "CHECKOUT_TELEMETRY_JSON_MAPPING",
            str(path),
        )
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
