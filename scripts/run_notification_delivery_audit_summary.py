from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.notification_delivery_audit_summary import (  # noqa: E402
    should_fail_cli,
    write_notification_delivery_audit_summary,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate read-only notification delivery audit summary."
    )
    parser.add_argument("--date", required=True, help="Audit date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default="data",
        help="Data root containing existing TRADING-030/031/034 artifacts.",
    )
    parser.add_argument(
        "--notification-draft-metadata-file",
        default=None,
        help="Explicit TRADING-030 notification draft metadata JSON path.",
    )
    parser.add_argument(
        "--delivery-preflight-file",
        default=None,
        help="Explicit TRADING-031 delivery preflight JSON path.",
    )
    parser.add_argument(
        "--dispatch-latest-file",
        default=None,
        help="Explicit TRADING-034 draft dispatch latest.json path.",
    )
    parser.add_argument(
        "--dispatch-file",
        default=None,
        help="Explicit TRADING-034 dated draft dispatch JSON path.",
    )
    parser.add_argument(
        "--fail-on-safety-anomaly",
        action="store_true",
        help="Exit non-zero on SAFETY_BLOCKED, MISMATCH, or ERROR.",
    )
    parser.add_argument(
        "--allow-missing-dispatch",
        action="store_true",
        help="Allow missing TRADING-034 dispatch artifact and emit PASS_WITH_WARNINGS.",
    )
    args = parser.parse_args()

    payload = write_notification_delivery_audit_summary(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        notification_draft_metadata_file=_optional_path(args.notification_draft_metadata_file),
        delivery_preflight_file=_optional_path(args.delivery_preflight_file),
        dispatch_latest_file=_optional_path(args.dispatch_latest_file),
        dispatch_file=_optional_path(args.dispatch_file),
        allow_missing_dispatch=args.allow_missing_dispatch,
    )
    outputs = payload["output_artifacts"]
    chain = payload["artifact_chain"]
    side_effects = payload["external_side_effect_audit"]
    print(f"Notification Delivery Audit Summary: {payload['audit_status']}")
    print(f"notification_lifecycle_status: {payload['notification_lifecycle_status']}")
    print(f"summary_level: {payload['summary_level']}")
    print(f"headline: {payload['headline']}")
    print(f"production_effect: {payload['production_effect']}")
    print(f"manual_review_only: {payload['manual_review_only']}")
    print(f"notification_delivery_audit_only: {payload['notification_delivery_audit_only']}")
    print(f"read_only: {payload['read_only']}")
    print(f"email_sent: {payload['email_sent']}")
    print(f"gmail_draft_created: {payload['gmail_draft_created']}")
    print(f"gmail_draft_modified: {payload['gmail_draft_modified']}")
    print(f"slack_sent: {payload['slack_sent']}")
    print(f"discord_sent: {payload['discord_sent']}")
    print(f"webhook_called: {payload['webhook_called']}")
    print(f"mobile_push_sent: {payload['mobile_push_sent']}")
    print(f"draft_hash_match: {chain['draft_hash_match']}")
    print(f"latest_json_match: {chain['dispatch_latest_match']}")
    print(f"external_side_effect_audit_status: {side_effects['status']}")
    print(f"Audit JSON: {outputs['audit_json']['path']}")
    print(f"Audit Markdown: {outputs['audit_markdown']['path']}")
    print(f"Run Log JSON: {outputs['run_log_json']['path']}")
    print(f"Run Log Markdown: {outputs['run_log_markdown']['path']}")
    if should_fail_cli(
        payload,
        fail_on_safety_anomaly=args.fail_on_safety_anomaly,
    ):
        raise SystemExit(2)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
