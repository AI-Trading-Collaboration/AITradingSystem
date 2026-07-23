from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.task_portfolio_normalization import (
    build_normalization_applied_closeout,
    build_normalization_decision_manifest,
    load_normalization_policy,
    validate_normalization_applied_closeout,
    validate_normalization_decision_manifest,
)
from ai_trading_system.platform.artifacts import write_json_atomic

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/governance/gov_006_wave1_normalization.yaml"
MANIFEST_PATH = PROJECT_ROOT / "inputs/governance/gov_006_wave1_decision_manifest.json"
APPLIED_CLOSEOUT_PATH = PROJECT_ROOT / "inputs/governance/gov_006_wave1_applied_closeout.json"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="GOV-006 active task portfolio normalization dry-run control"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("generate")
    subparsers.add_parser("validate")
    generate_applied = subparsers.add_parser("generate-applied")
    generate_applied.add_argument("--application-commit", required=True)
    subparsers.add_parser("validate-applied")
    args = parser.parse_args()
    if args.command == "generate":
        policy = load_normalization_policy(POLICY_PATH)
        payload = build_normalization_decision_manifest(
            project_root=PROJECT_ROOT,
            policy=policy,
            policy_path=POLICY_PATH,
        )
        write_json_atomic(MANIFEST_PATH, payload)
        _print_dry_run_summary(payload)
        return 0
    if args.command == "validate":
        policy = load_normalization_policy(POLICY_PATH)
        payload = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        validate_normalization_decision_manifest(
            payload,
            project_root=PROJECT_ROOT,
            policy=policy,
            policy_path=POLICY_PATH,
        )
        _print_dry_run_summary(payload)
        return 0
    if args.command == "generate-applied":
        payload = build_normalization_applied_closeout(
            project_root=PROJECT_ROOT,
            policy={},
            policy_path=POLICY_PATH,
            decision_manifest_path=MANIFEST_PATH,
            application_commit=args.application_commit,
        )
        write_json_atomic(APPLIED_CLOSEOUT_PATH, payload)
        _print_applied_summary(payload)
        return 0
    payload = json.loads(APPLIED_CLOSEOUT_PATH.read_text(encoding="utf-8"))
    validate_normalization_applied_closeout(
        payload,
        project_root=PROJECT_ROOT,
        policy={},
        policy_path=POLICY_PATH,
        decision_manifest_path=MANIFEST_PATH,
    )
    _print_applied_summary(payload)
    return 0


def _print_dry_run_summary(payload: Mapping[str, Any]) -> None:
    source = _mapping(payload["source"], "source")
    print(
        json.dumps(
            {
                "status": "PASS",
                "mode": "DRY_RUN_ONLY",
                "manifest_path": str(MANIFEST_PATH),
                "manifest_id": payload["manifest_id"],
                "manifest_sha256": payload["manifest_sha256"],
                "base_commit": source["base_commit"],
                "decision_count": payload["decision_count"],
                "automatic_apply_allowed": False,
                "production_effect": "none",
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _print_applied_summary(payload: Mapping[str, Any]) -> None:
    lineage = _mapping(payload["lineage"], "lineage")
    application = _mapping(payload["application"], "application")
    before = _mapping(payload["before_inventory"], "before_inventory")
    after = _mapping(payload["after_inventory"], "after_inventory")
    safety = _mapping(payload["safety"], "safety")
    print(
        json.dumps(
            {
                "status": "PASS",
                "mode": "APPLIED_CLOSEOUT",
                "closeout_path": str(APPLIED_CLOSEOUT_PATH),
                "closeout_id": payload["closeout_id"],
                "closeout_sha256": payload["closeout_sha256"],
                "historical_base_commit": lineage["historical_base_commit"],
                "application_commit": lineage["application_commit"],
                "decision_count": application["decision_count"],
                "active_task_count_before": before["active_task_count"],
                "active_task_count_after": after["active_task_count"],
                "completed_task_count_after": after["completed_task_count"],
                "production_effect": safety["production_effect"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SystemExit(f"{field} must be a mapping")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
