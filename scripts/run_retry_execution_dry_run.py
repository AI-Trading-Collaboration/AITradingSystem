from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.retry_execution_dry_run import (  # noqa: E402
    should_fail_cli,
    write_retry_execution_dry_run,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate manual approval record summary and retry execution dry-run report."
    )
    parser.add_argument(
        "--queue-report",
        default=None,
        help="Explicit TRADING-037 retry candidate queue JSON path.",
    )
    parser.add_argument(
        "--approval-record",
        default=None,
        help="Optional manual retry approval record JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/retry_execution_dry_run",
        help="Directory for TRADING-038 JSON, Markdown, and log artifacts.",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Dry-run report date in YYYY-MM-DD format; defaults to source queue date.",
    )
    parser.add_argument(
        "--fail-on-safety-blocked",
        action="store_true",
        help="Exit non-zero when dry_run_status is SAFETY_BLOCKED.",
    )
    parser.add_argument(
        "--fail-on-approval-mismatch",
        action="store_true",
        help="Exit non-zero when dry_run_status is APPROVAL_MISMATCH.",
    )
    args = parser.parse_args()

    payload = write_retry_execution_dry_run(
        as_of=None if args.as_of_date in (None, "") else date.fromisoformat(args.as_of_date),
        queue_report_path=_optional_path(args.queue_report),
        approval_record_path=_optional_path(args.approval_record),
        output_dir=Path(args.output_dir),
    )
    outputs = payload["output_artifacts"]
    source = payload["source_queue"]
    approval = payload["approval_record"]
    summary = payload["dry_run_summary"]
    print(f"Retry Execution Dry Run: {summary['dry_run_status']}")
    print(f"source_queue_status: {source['queue_status']}")
    print(f"source_parse_status: {source['source_parse_status']}")
    print(f"approval_record_available: {approval['approval_record_available']}")
    print(f"approval_parse_status: {approval['approval_parse_status']}")
    print(f"total_candidates: {summary['total_candidates']}")
    print(f"approved_for_dry_run: {summary['approved_for_dry_run']}")
    print(f"blocked_from_dry_run: {summary['blocked_from_dry_run']}")
    print(f"simulated_retry_actions: {summary['simulated_retry_actions']}")
    print(f"real_retry_allowed: {summary['real_retry_allowed']}")
    print(f"external_delivery_allowed: {summary['external_delivery_allowed']}")
    print(f"production_state_mutation_allowed: {summary['production_state_mutation_allowed']}")
    print(f"production_effect: {payload['production_effect']}")
    print(f"manual_review_only: {payload['manual_review_only']}")
    print(f"dry_run_only: {payload['dry_run_only']}")
    print(f"Dry Run JSON: {outputs['retry_execution_dry_run_json']['path']}")
    print(f"Dry Run Markdown: {outputs['retry_execution_dry_run_markdown']['path']}")
    print(f"Run Log: {outputs['run_log']['path']}")
    if should_fail_cli(
        payload,
        fail_on_safety_blocked=args.fail_on_safety_blocked,
        fail_on_approval_mismatch=args.fail_on_approval_mismatch,
    ):
        raise SystemExit(2)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
