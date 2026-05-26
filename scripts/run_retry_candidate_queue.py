from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.retry_candidate_queue import (  # noqa: E402
    should_fail_cli,
    write_retry_candidate_queue,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate read-only retry candidate queue and manual approval gate."
    )
    parser.add_argument(
        "--classification-report",
        default=None,
        help="Explicit TRADING-036 notification delivery failure classification JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/retry_candidate_queue",
        help="Directory for TRADING-037 JSON, Markdown, and log artifacts.",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Queue date in YYYY-MM-DD format; defaults to source classification date.",
    )
    parser.add_argument(
        "--fail-on-safety-blocked",
        action="store_true",
        help="Exit non-zero when queue_status is SAFETY_BLOCKED.",
    )
    args = parser.parse_args()

    payload = write_retry_candidate_queue(
        as_of=None if args.as_of_date in (None, "") else date.fromisoformat(args.as_of_date),
        classification_report_path=_optional_path(args.classification_report),
        output_dir=Path(args.output_dir),
    )
    outputs = payload["output_artifacts"]
    source = payload["source_classification"]
    summary = payload["queue_summary"]
    approval = payload["approval_gate"]
    print(f"Retry Candidate Queue: {summary['queue_status']}")
    print(f"source_overall_status: {source['overall_status']}")
    print(f"source_highest_severity: {source['highest_severity']}")
    print(f"source_parse_status: {source['source_parse_status']}")
    print(f"total_candidates: {summary['total_candidates']}")
    print(f"blocked_candidates: {summary['blocked_candidates']}")
    print(f"manual_review_required: {summary['manual_review_required']}")
    print(f"has_retryable_candidates: {summary['has_retryable_candidates']}")
    print(f"safe_to_execute_retry: {summary['safe_to_execute_retry']}")
    print(f"approval_status: {approval['approval_status']}")
    print(f"retry_execution_allowed: {approval['retry_execution_allowed']}")
    print(f"production_effect: {payload['production_effect']}")
    print(f"manual_review_only: {payload['manual_review_only']}")
    print(f"read_only: {payload['read_only']}")
    print(f"Queue JSON: {outputs['retry_candidate_queue_json']['path']}")
    print(f"Queue Markdown: {outputs['retry_candidate_queue_markdown']['path']}")
    print(f"Run Log: {outputs['run_log']['path']}")
    if should_fail_cli(payload, fail_on_safety_blocked=args.fail_on_safety_blocked):
        raise SystemExit(2)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
