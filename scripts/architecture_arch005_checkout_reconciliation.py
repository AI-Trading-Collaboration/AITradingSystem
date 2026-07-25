from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.checkout_guard import CheckoutGuardError
from ai_trading_system.platform.architecture.checkout_reconciliation import (
    CheckoutHandoffMode,
    build_checkout_handoff,
    build_checkout_reconciliation_report,
    validate_checkout_handoff,
    validate_checkout_reconciliation_report,
    write_checkout_handoff,
    write_checkout_reconciliation_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ARCH-005S4E checkout handoff and source reconciliation"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command, mode in (
        ("prepare", CheckoutHandoffMode.PREPARED_COPY),
        ("prepare-recovery", CheckoutHandoffMode.RECOVERY_AUDIT),
    ):
        prepare = subparsers.add_parser(command)
        prepare.set_defaults(mode=mode)
        prepare.add_argument("--source-root", type=Path, required=True)
        prepare.add_argument("--target-root", type=Path, required=True)
        prepare.add_argument("--task-id", required=True)
        prepare.add_argument("--target-ref", required=True)
        prepare.add_argument("--owned-path", action="append", default=[])
        prepare.add_argument("--generated-path", action="append", default=[])
        prepare.add_argument("--retained-path", action="append", default=[])
        prepare.add_argument("--output", type=Path, required=True)
        prepare.add_argument("--at")

    audit = subparsers.add_parser("audit")
    audit.add_argument("--manifest", type=Path, required=True)
    audit.add_argument("--source-root", type=Path)
    audit.add_argument("--target-root", type=Path)
    audit.add_argument("--target-ref")
    audit.add_argument("--output", type=Path, required=True)
    audit.add_argument("--at")

    validate_handoff = subparsers.add_parser("validate-handoff")
    validate_handoff.add_argument("--manifest", type=Path, required=True)

    validate_report = subparsers.add_parser("validate-report")
    validate_report.add_argument("--report", type=Path, required=True)

    args = parser.parse_args()
    try:
        if args.command in {"prepare", "prepare-recovery"}:
            payload = build_checkout_handoff(
                source_root=args.source_root,
                target_root=args.target_root,
                task_id=args.task_id,
                target_ref=args.target_ref,
                owned_paths=tuple(args.owned_path),
                generated_paths=tuple(args.generated_path),
                retained_paths=tuple(args.retained_path),
                mode=args.mode,
                created_at=_parse_datetime(args.at),
            )
            write_checkout_handoff(args.output, payload)
        elif args.command == "audit":
            handoff = _load_json(args.manifest)
            payload = build_checkout_reconciliation_report(
                handoff=handoff,
                source_root=args.source_root,
                target_root=args.target_root,
                target_ref=args.target_ref,
                created_at=_parse_datetime(args.at),
            )
            write_checkout_reconciliation_report(args.output, payload)
        elif args.command == "validate-handoff":
            payload = _load_json(args.manifest)
            validate_checkout_handoff(payload)
            payload = {
                "status": "PASS",
                "artifact": str(args.manifest),
                "schema_version": payload.get("schema_version"),
                "production_effect": "none",
            }
        else:
            payload = _load_json(args.report)
            validate_checkout_reconciliation_report(payload)
            payload = {
                "status": "PASS",
                "artifact": str(args.report),
                "schema_version": payload.get("schema_version"),
                "production_effect": "none",
            }
    except (CheckoutGuardError, OSError, json.JSONDecodeError) as exc:
        code = exc.code if isinstance(exc, CheckoutGuardError) else type(exc).__name__
        message = exc.message if isinstance(exc, CheckoutGuardError) else str(exc)
        payload = {
            "status": "BLOCKED",
            "error_code": code,
            "message": message,
            "automatic_cleanup_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") == "PASS" else 1


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_JSON_MAPPING",
            str(path),
        )
    return payload


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--at must include a timezone")
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
