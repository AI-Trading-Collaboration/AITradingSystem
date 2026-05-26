from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.notification_delivery_failure_classification import (  # noqa: E402
    should_fail_cli,
    write_notification_delivery_failure_classification,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate read-only notification delivery failure classification."
    )
    parser.add_argument(
        "--audit-summary",
        default=None,
        help="Explicit TRADING-035 notification delivery audit summary JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/notification_delivery_failure_classification",
        help="Directory for TRADING-036 JSON, Markdown, and log artifacts.",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Classification date in YYYY-MM-DD format; defaults to source audit date.",
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Exit non-zero when classification highest severity is CRITICAL.",
    )
    args = parser.parse_args()

    payload = write_notification_delivery_failure_classification(
        as_of=None if args.as_of_date in (None, "") else date.fromisoformat(args.as_of_date),
        audit_summary_path=_optional_path(args.audit_summary),
        output_dir=Path(args.output_dir),
    )
    outputs = payload["output_artifacts"]
    source = payload["source_audit"]
    summary = payload["classification_summary"]
    retry = payload["retry_readiness"]
    print(f"Notification Delivery Failure Classification: {summary['overall_status']}")
    print(f"source_audit_status: {source['audit_status']}")
    print(f"source_parse_status: {source['source_parse_status']}")
    print(f"highest_severity: {summary['highest_severity']}")
    print(f"total_failures: {summary['total_failures']}")
    print(f"requires_manual_review: {summary['requires_manual_review']}")
    print(f"safe_to_retry: {summary['safe_to_retry']}")
    print(f"blocks_notification_chain: {summary['blocks_notification_chain']}")
    print(f"retry_mode: {retry['retry_mode']}")
    print(f"production_effect: {payload['production_effect']}")
    print(f"manual_review_only: {payload['manual_review_only']}")
    print(f"read_only: {payload['read_only']}")
    print(f"Classification JSON: {outputs['classification_json']['path']}")
    print(f"Classification Markdown: {outputs['classification_markdown']['path']}")
    print(f"Run Log: {outputs['run_log']['path']}")
    if should_fail_cli(payload, fail_on_critical=args.fail_on_critical):
        raise SystemExit(2)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
