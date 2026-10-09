#!/usr/bin/env python3
"""DEVX-023 S: build, verify and describe the lease-store replay seal.

`build` takes the lease-store arbiter, validates every chain in full (no seal is consulted) and
writes `replay_seal.v1.json` into the store root with one atomic replace. `verify` and `status`
only read: `verify` compares the sealed replay with the serial full replay field by field. The seal
is used by a process only when it sets AITS_LEASE_SEAL=1 (default off). See
docs/requirements/DEVX-023_Lease_Replay_Cost_Reduction_V1.md section 10.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ai_trading_system.platform.architecture import lease_replay_seal  # noqa: E402
from ai_trading_system.platform.architecture.checkout_guard import (  # noqa: E402
    CheckoutGuardError,
    CheckoutLeaseGuard,
)
from ai_trading_system.platform.architecture.parallel_control import (  # noqa: E402
    ParallelControlError,
)

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_ERROR = 2
SAFETY = {
    "production_effect": "none",
    "broker_action": "none",
    "task_governance_status_mutated": False,
}


def _parse_instant(value: str | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("--at must be timezone-aware")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=PROJECT_ROOT)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build", help="validate every chain in full and write the seal")
    build.add_argument("--actor", default="integration-coordinator")
    build.add_argument("--at")
    commands.add_parser("verify", help="sealed replay against the serial full replay (read-only)")
    status = commands.add_parser("status", help="why the seal is or is not in force (read-only)")
    status.add_argument("--no-replay", action="store_true")
    args = parser.parse_args(argv)
    try:
        store = CheckoutLeaseGuard(project_root=args.repository).store
        if args.command == "build":
            now = _parse_instant(args.at)
            with store._arbiter(actor=args.actor, now=now, operation="seal"):
                document, report = lease_replay_seal.build_seal_document(
                    store,
                    created_at=now,
                    created_by="architecture_arch005_lease_seal.py build",
                )
                lease_replay_seal.write_seal(
                    store.root / lease_replay_seal.SEAL_FILE_NAME, document
                )
            payload = {
                "status": "PASS",
                "command": "build",
                "seal_path": str(store.root / lease_replay_seal.SEAL_FILE_NAME),
                "seal_sha256": document["seal_sha256"],
                "kernel_fingerprint": document["kernel_fingerprint"],
                "report": report.to_dict(),
                **SAFETY,
            }
            code = EXIT_PASS
        elif args.command == "verify":
            result = lease_replay_seal.verify(store)
            payload = {
                "status": "PASS" if result.ok else "FAIL",
                "command": "verify",
                **result.to_dict(),
                **SAFETY,
            }
            code = EXIT_PASS if result.ok else EXIT_FAIL
        else:
            payload = {
                "status": "PASS",
                "command": "status",
                **lease_replay_seal.status(store, replay=not args.no_replay),
                **SAFETY,
            }
            code = EXIT_PASS
    except (CheckoutGuardError, ParallelControlError, OSError, ValueError) as error:
        payload = {
            "status": "ERROR",
            "code": getattr(error, "code", type(error).__name__),
            "message": str(error),
            **SAFETY,
        }
        code = EXIT_ERROR
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
