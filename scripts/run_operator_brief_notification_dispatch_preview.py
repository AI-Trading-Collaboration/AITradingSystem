from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.operator_brief_notification_dispatch_preview import (  # noqa: E402
    DISPATCH_BLOCKED,
    DISPATCH_SAFETY_BLOCKED,
    should_fail_cli,
    write_operator_brief_notification_dispatch_preview,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate read-only operator brief notification dry-run dispatch preview."
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Preview date in YYYY-MM-DD format. Defaults to today's local date.",
    )
    parser.add_argument(
        "--data-root",
        default="data",
        help="Data root containing existing TRADING-031 and operator brief artifacts.",
    )
    parser.add_argument(
        "--input-preflight",
        default=None,
        help="Explicit TRADING-031 preflight JSON path.",
    )
    parser.add_argument(
        "--operator-brief-json",
        default=None,
        help="Explicit TRADING-022 operator brief JSON path.",
    )
    parser.add_argument(
        "--operator-brief-markdown",
        default=None,
        help="Explicit TRADING-022 operator brief Markdown path.",
    )
    parser.add_argument(
        "--notification-draft-metadata-file",
        default=None,
        help="Explicit TRADING-030 notification draft metadata JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for dispatch preview artifacts. Must stay inside repo root.",
    )
    args = parser.parse_args()

    try:
        as_of = date.fromisoformat(args.date) if args.date else date.today()
        payload = write_operator_brief_notification_dispatch_preview(
            as_of=as_of,
            data_root=Path(args.data_root),
            input_preflight_file=_optional_path(args.input_preflight),
            operator_brief_json_file=_optional_path(args.operator_brief_json),
            operator_brief_markdown_file=_optional_path(args.operator_brief_markdown),
            notification_draft_metadata_file=_optional_path(args.notification_draft_metadata_file),
            output_dir=_optional_path(args.output_dir),
        )
    except Exception as exc:
        print("FAIL: operator brief notification dispatch preview failed")
        print(f"error: {exc}")
        raise SystemExit(1) from exc

    decision = payload["decision"]
    preview = payload["dispatch_preview"]
    outputs = payload["output_artifacts"]
    final_status = decision["final_status"]
    if final_status in {DISPATCH_BLOCKED, DISPATCH_SAFETY_BLOCKED}:
        print("LIMITED: operator brief notification dispatch preview generated with blocked status")
    else:
        print("PASS: operator brief notification dispatch preview generated")
    print(f"final_status: {final_status}")
    print(f"dispatch_status: {preview['dispatch_status']}")
    print(f"human_action_required: {decision['human_action_required']}")
    print(f"next_recommended_action: {decision['next_recommended_action']}")
    print(f"production_effect: {payload['production_effect']}")
    print(f"manual_review_only: {payload['manual_review_only']}")
    print(f"dispatch_preview_only: {payload['dispatch_preview_only']}")
    print(f"external_side_effects: {payload['external_side_effects']}")
    print(f"network_access_required: {payload['network_access_required']}")
    print(f"secrets_required: {payload['secrets_required']}")
    print(f"email_sent: {payload['email_sent']}")
    print(f"gmail_draft_created: {payload['gmail_draft_created']}")
    print(f"slack_sent: {payload['slack_sent']}")
    print(f"telegram_sent: {payload['telegram_sent']}")
    print(f"discord_sent: {payload['discord_sent']}")
    print(f"webhook_called: {payload['webhook_called']}")
    print(f"mobile_push_sent: {payload['mobile_push_sent']}")
    print(f"Dispatch Preview JSON: {outputs['dispatch_preview_json']['path']}")
    print(f"Dispatch Preview Markdown: {outputs['dispatch_preview_markdown']['path']}")
    print(f"Latest JSON: {outputs['latest_json']['path']}")
    print(f"Latest Markdown: {outputs['latest_markdown']['path']}")
    print(f"Run Log: {outputs['run_log']['path']}")
    if should_fail_cli(payload):
        raise SystemExit(1)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
