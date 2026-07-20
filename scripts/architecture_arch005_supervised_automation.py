from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.platform.architecture.supervised_automation import (
    SupervisedAutomationController,
    audit_supervised_orphans,
    cleanup_clean_supervised_worktrees,
    validate_supervised_run,
)
from ai_trading_system.platform.artifacts import write_json_atomic

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_005_supervised_automation_policy.yaml"
PILOT_PATH = PROJECT_ROOT / "inputs/architecture/arch_005_s4a_supervised_pilot.yaml"
RUNTIME_ROOT = PROJECT_ROOT / "outputs/architecture/arch_005_s4a"


def main() -> int:
    parser = argparse.ArgumentParser(description="ARCH-005 S4A supervised automation")
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--base-commit")
    run = subparsers.add_parser("run")
    run.add_argument("--started-at")
    validate = subparsers.add_parser("validate")
    validate.add_argument("--report", type=Path, required=True)
    orphan = subparsers.add_parser("orphan-audit")
    orphan.add_argument("--report", type=Path, required=True)
    cleanup = subparsers.add_parser("cleanup-clean")
    cleanup.add_argument("--report", type=Path, required=True)
    cleanup.add_argument("--coordinator-approved", action="store_true")
    args = parser.parse_args()
    controller = SupervisedAutomationController(
        project_root=PROJECT_ROOT,
        runtime_root=RUNTIME_ROOT,
        policy_path=POLICY_PATH,
        pilot_path=PILOT_PATH,
    )
    if args.command == "plan":
        payload = controller.plan(base_commit=args.base_commit)
    elif args.command == "run":
        started_at = (
            datetime.fromisoformat(args.started_at) if args.started_at else datetime.now(UTC)
        )
        report_path = controller.run(started_at=started_at)
        payload = {
            "status": "PASS",
            "report_path": report_path.relative_to(PROJECT_ROOT).as_posix(),
            "production_effect": "none",
        }
    elif args.command == "validate":
        payload = validate_supervised_run(
            args.report,
            project_root=PROJECT_ROOT,
            policy_path=POLICY_PATH,
            pilot_path=PILOT_PATH,
        )
        write_json_atomic(args.report.parent / "supervised_run_validation.json", payload)
    elif args.command == "orphan-audit":
        payload = audit_supervised_orphans(
            args.report,
            project_root=PROJECT_ROOT,
            policy_path=POLICY_PATH,
        )
        write_json_atomic(args.report.parent / "supervised_orphan_audit.json", payload)
    else:
        payload = cleanup_clean_supervised_worktrees(
            args.report,
            project_root=PROJECT_ROOT,
            policy_path=POLICY_PATH,
            coordinator_approved=args.coordinator_approved,
        )
        write_json_atomic(args.report.parent / "supervised_cleanup_result.json", payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
