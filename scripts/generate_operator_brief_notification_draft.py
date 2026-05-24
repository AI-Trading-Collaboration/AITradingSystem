from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.operator_brief_notification_draft import (  # noqa: E402
    should_fail_cli,
    write_operator_brief_notification_draft,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate review-only operator brief notification drafts."
    )
    parser.add_argument("--date", required=True, help="Draft date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default="data",
        help="Data root containing existing operator brief and upstream artifacts.",
    )
    parser.add_argument(
        "--operator-brief-file",
        default=None,
        help="Explicit TRADING-022 operator brief JSON path.",
    )
    parser.add_argument(
        "--parameter-governance-digest-file",
        default=None,
        help="Optional TRADING-021 parameter governance digest JSON path.",
    )
    parser.add_argument(
        "--pipeline-health-summary-file",
        default=None,
        help="Optional TRADING-023 pipeline health summary JSON path.",
    )
    parser.add_argument(
        "--data-freshness-summary-file",
        default=None,
        help="Optional TRADING-024 data freshness summary JSON path.",
    )
    parser.add_argument(
        "--scheduler-dry-run-file",
        default=None,
        help="Optional TRADING-026 scheduler dry-run JSON path.",
    )
    parser.add_argument(
        "--audience",
        default="personal",
        help="Audience label recorded in metadata. Draft remains manual-review only.",
    )
    parser.add_argument(
        "--max-lines",
        type=int,
        default=20,
        help="Soft line budget for copied notification sections.",
    )
    parser.add_argument(
        "--include-links",
        action="store_true",
        help="Include source artifact paths in the email draft Links section.",
    )
    parser.add_argument(
        "--fail-on-urgent",
        action="store_true",
        help="Exit non-zero when generated notification severity is URGENT.",
    )
    args = parser.parse_args()

    payload = write_operator_brief_notification_draft(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        operator_brief_file=_optional_path(args.operator_brief_file),
        parameter_governance_digest_file=_optional_path(args.parameter_governance_digest_file),
        pipeline_health_summary_file=_optional_path(args.pipeline_health_summary_file),
        data_freshness_summary_file=_optional_path(args.data_freshness_summary_file),
        scheduler_dry_run_file=_optional_path(args.scheduler_dry_run_file),
        audience=args.audience,
        max_lines=args.max_lines,
        include_links=args.include_links,
        fail_on_urgent=False,
    )
    outputs = payload["draft_outputs"]
    content = payload["notification_content_summary"]
    print(f"Operator Brief Notification Draft：{payload['draft_status']}")
    print(f"notification_severity：{payload['notification_severity']}")
    print(f"headline：{payload['headline']}")
    print(f"email_sent：{payload['email_sent']}")
    print(f"gmail_draft_created：{payload['gmail_draft_created']}")
    print(f"slack_sent：{payload['slack_sent']}")
    print(f"discord_sent：{payload['discord_sent']}")
    print(f"mobile_push_sent：{payload['mobile_push_sent']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"notification_draft_only：{payload['notification_draft_only']}")
    print(f"read_only：{payload['read_only']}")
    print(
        "operator_brief_executed_by_notification_draft："
        f"{payload['operator_brief_executed_by_notification_draft']}"
    )
    print(
        "pipelines_executed_by_notification_draft："
        f"{payload['pipelines_executed_by_notification_draft']}"
    )
    print(
        "data_downloaded_by_notification_draft："
        f"{payload['data_downloaded_by_notification_draft']}"
    )
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"email_lines：{content['email_lines']}")
    print(f"chat_lines：{content['chat_lines']}")
    print(f"mobile_lines：{content['mobile_lines']}")
    print(f"Notification Metadata：{payload['output_artifacts']['metadata_json']['path']}")
    print(f"Summary Markdown：{outputs['summary_markdown']['path']}")
    print(f"Email Draft：{outputs['email_draft']['path']}")
    print(f"Chat Draft：{outputs['chat_draft']['path']}")
    print(f"Mobile Summary：{outputs['mobile_summary']['path']}")
    if should_fail_cli(payload, fail_on_urgent=args.fail_on_urgent):
        raise SystemExit(2)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
