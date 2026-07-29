from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.data.foundation_consumer_migration import (  # noqa: E402
    DEFAULT_ACL_POLICY_PATH,
    DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH,
    DEFAULT_DQ_POLICY_PATH,
    DEFAULT_POLICY_PATH,
    run_isolated_consumer_migration_rehearsal,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "在 isolated candidate root 重放 exact daily_score_daily publication，"
            "并验收 strict DQ、durability、ACL 与 consumer authorization。"
        )
    )
    parser.add_argument("--source-project-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--generated-at", type=_parse_generated_at, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--acl-policy", type=Path, default=DEFAULT_ACL_POLICY_PATH)
    parser.add_argument("--data-quality-policy", type=Path, default=DEFAULT_DQ_POLICY_PATH)
    parser.add_argument(
        "--consumer-authorization-policy",
        type=Path,
        default=DEFAULT_CONSUMER_AUTHORIZATION_POLICY_PATH,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    bundle = run_isolated_consumer_migration_rehearsal(
        source_project_root=args.source_project_root,
        output_root=args.output_dir,
        project_root=PROJECT_ROOT,
        generated_at=args.generated_at,
        policy_path=args.policy,
        acl_policy_path=args.acl_policy,
        data_quality_policy_path=args.data_quality_policy,
        consumer_authorization_policy_path=args.consumer_authorization_policy,
    )
    print(
        json.dumps(
            {
                "status": bundle["status"],
                "bundle_id": bundle["bundle_id"],
                "consumer_id": bundle["consumer_id"],
                "consumer_version": bundle["consumer_version"],
                "candidate_project_root": bundle["candidate_project_root"],
                "candidate_data_root": bundle["candidate_data_root"],
                "production_effect": "none",
                "broker_action": "none",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _parse_generated_at(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("generated-at must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("generated-at must include a UTC offset")
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
