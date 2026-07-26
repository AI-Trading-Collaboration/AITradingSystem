#!/usr/bin/env python3
"""Build or validate a read-only base-drift integration plan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.integration_revalidation import (
    DEFAULT_INTEGRATION_REVALIDATION_POLICY_PATH,
    IntegrationRevalidationError,
    build_integration_revalidation_plan,
    validate_integration_revalidation_plan,
    write_integration_revalidation_plan,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ARCH-005 base-drift-aware integration revalidation planner"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "validate"):
        current = subparsers.add_parser(command)
        current.add_argument("--repository", type=Path, required=True)
        current.add_argument("--manifest", type=Path, required=True)
        current.add_argument(
            "--policy",
            type=Path,
            default=DEFAULT_INTEGRATION_REVALIDATION_POLICY_PATH,
        )
        if command == "plan":
            current.add_argument("--frozen-base", required=True)
            current.add_argument("--lane-head", required=True)
            current.add_argument("--latest-main", required=True)
            current.add_argument(
                "--mainline-contract-claims",
                type=Path,
            )
            current.add_argument("--output", type=Path)
        else:
            current.add_argument("--plan", type=Path, required=True)
    return parser


def _load_object(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise IntegrationRevalidationError(f"{field}_READ", str(exc)) from exc
    if not isinstance(value, dict):
        raise IntegrationRevalidationError(f"{field}_ROOT", "expected JSON object")
    return value


def _load_claims(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise IntegrationRevalidationError("CLAIMS_READ", str(exc)) from exc
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise IntegrationRevalidationError(
            "CLAIMS_ROOT",
            "mainline contract claims must be a JSON list of objects",
        )
    return value


def main() -> int:
    args = _parser().parse_args()
    try:
        manifest = _load_object(args.manifest, "MANIFEST")
        if args.command == "plan":
            payload = build_integration_revalidation_plan(
                repository=args.repository,
                frozen_base=args.frozen_base,
                lane_head=args.lane_head,
                latest_main=args.latest_main,
                manifest=manifest,
                policy_path=args.policy,
                mainline_contract_claims=_load_claims(args.mainline_contract_claims),
            )
            if args.output is not None:
                write_integration_revalidation_plan(args.output, payload)
            print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))
            return 0 if payload["decision"] == "READY_FOR_SINGLE_INTEGRATION_CANDIDATE" else 2
        payload = _load_object(args.plan, "PLAN")
        validate_integration_revalidation_plan(
            payload,
            repository=args.repository,
            manifest=manifest,
            policy_path=args.policy,
        )
        print(
            json.dumps(
                {
                    "schema_version": "integration_revalidation_validation.v1",
                    "status": "PASS",
                    "plan_id": payload["plan_id"],
                    "plan_sha256": payload["plan_sha256"],
                    "production_effect": "none",
                    "broker_action": "none",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except IntegrationRevalidationError as exc:
        print(
            json.dumps(
                {
                    "schema_version": "integration_revalidation_error.v1",
                    "status": "BLOCKED",
                    "code": exc.code,
                    "message": exc.message,
                    "production_effect": "none",
                    "broker_action": "none",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
