from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.operator_brief_notification_delivery_preflight import (  # noqa: E402
    should_fail_cli,
    write_operator_brief_notification_delivery_preflight,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run read-only operator brief notification delivery preflight."
    )
    parser.add_argument("--date", required=True, help="Preflight date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default="data",
        help="Data root containing existing TRADING-030 notification draft artifacts.",
    )
    parser.add_argument(
        "--notification-draft-metadata-file",
        default=None,
        help="Explicit TRADING-030 notification draft metadata JSON path.",
    )
    parser.add_argument(
        "--recipient-config-file",
        default=None,
        help="Optional recipient config JSON path.",
    )
    parser.add_argument(
        "--channel-config-file",
        default=None,
        help="Optional channel config JSON path.",
    )
    parser.add_argument(
        "--approval-config-file",
        default=None,
        help="Optional approval policy config JSON path.",
    )
    parser.add_argument(
        "--allow-missing-recipient-config",
        action="store_true",
        help="Suppress recipient config missing warning text in alerts.",
    )
    parser.add_argument(
        "--allow-missing-channel-config",
        action="store_true",
        help="Suppress channel config missing warning text in alerts.",
    )
    parser.add_argument(
        "--fail-on-urgent-without-approval",
        action="store_true",
        help="Block when URGENT severity lacks an approval policy.",
    )
    args = parser.parse_args()

    payload = write_operator_brief_notification_delivery_preflight(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        notification_draft_metadata_file=_optional_path(args.notification_draft_metadata_file),
        recipient_config_file=_optional_path(args.recipient_config_file),
        channel_config_file=_optional_path(args.channel_config_file),
        approval_config_file=_optional_path(args.approval_config_file),
        allow_missing_recipient_config=args.allow_missing_recipient_config,
        allow_missing_channel_config=args.allow_missing_channel_config,
        fail_on_urgent_without_approval=args.fail_on_urgent_without_approval,
    )
    outputs = payload["output_artifacts"]
    print(f"Operator Brief Notification Delivery Preflight: {payload['preflight_status']}")
    print(f"delivery_readiness: {payload['delivery_readiness']}")
    print(f"notification_severity: {payload['notification_severity']}")
    print(f"headline: {payload['headline']}")
    print(f"production_effect: {payload['production_effect']}")
    print(f"manual_review_only: {payload['manual_review_only']}")
    print(
        "notification_delivery_preflight_only: "
        f"{payload['notification_delivery_preflight_only']}"
    )
    print(f"read_only: {payload['read_only']}")
    print(f"email_sent: {payload['email_sent']}")
    print(f"gmail_draft_created: {payload['gmail_draft_created']}")
    print(f"gmail_draft_modified: {payload['gmail_draft_modified']}")
    print(f"slack_sent: {payload['slack_sent']}")
    print(f"discord_sent: {payload['discord_sent']}")
    print(f"webhook_called: {payload['webhook_called']}")
    print(f"mobile_push_sent: {payload['mobile_push_sent']}")
    print(
        "operator_brief_executed_by_delivery_preflight: "
        f"{payload['operator_brief_executed_by_delivery_preflight']}"
    )
    print(
        "notification_draft_executed_by_delivery_preflight: "
        f"{payload['notification_draft_executed_by_delivery_preflight']}"
    )
    print(
        "pipelines_executed_by_delivery_preflight: "
        f"{payload['pipelines_executed_by_delivery_preflight']}"
    )
    print(
        "data_downloaded_by_delivery_preflight: "
        f"{payload['data_downloaded_by_delivery_preflight']}"
    )
    print(f"broker_execution: {payload['broker_execution']}")
    print(f"replay_execution: {payload['replay_execution']}")
    print(f"trading_execution: {payload['trading_execution']}")
    print(f"Preflight JSON: {outputs['preflight_json']['path']}")
    print(f"Preflight Markdown: {outputs['preflight_markdown']['path']}")
    if should_fail_cli(payload):
        raise SystemExit(2)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
