from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.operator_brief_notification_approval_gate import (  # noqa: E402
    APPROVED,
    should_fail_cli,
    write_operator_brief_notification_approval_gate,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate read-only operator brief notification approval gate artifact."
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Approval gate date in YYYY-MM-DD format. Defaults to today's local date.",
    )
    parser.add_argument(
        "--data-root",
        default="data",
        help="Data root containing existing TRADING-032 dispatch preview artifacts.",
    )
    parser.add_argument(
        "--input-preview",
        default=None,
        help="Explicit TRADING-032 dispatch preview JSON path.",
    )
    parser.add_argument(
        "--approval-marker",
        default=None,
        help="Explicit local manual approval marker JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for approval gate artifacts. Must stay inside repo root.",
    )
    args = parser.parse_args()

    try:
        as_of = date.fromisoformat(args.date) if args.date else date.today()
        payload = write_operator_brief_notification_approval_gate(
            as_of=as_of,
            data_root=Path(args.data_root),
            input_preview_file=_optional_path(args.input_preview),
            approval_marker_file=_optional_path(args.approval_marker),
            output_dir=_optional_path(args.output_dir),
        )
    except Exception as exc:
        print("FAIL: operator brief notification approval gate failed")
        print(f"error: {exc}")
        raise SystemExit(1) from exc

    decision = payload["decision"]
    preview = payload["dispatch_preview_summary"]
    marker = payload["approval_marker_summary"]
    outputs = payload["output_artifacts"]
    status = decision["approval_gate_status"]
    if status == APPROVED:
        print("PASS: operator brief notification approval gate generated")
    else:
        print(
            "LIMITED: operator brief notification approval gate generated "
            "with non-approved status"
        )
    print(f"approval_gate_status: {status}")
    print(f"allowed_to_enter_dispatch: {decision['allowed_to_enter_dispatch']}")
    print(f"human_action_required: {decision['human_action_required']}")
    print(f"dispatch_preview_status: {preview['final_status']}")
    print(f"approval_marker_exists: {marker['exists']}")
    print(f"hash_matches: {marker['hash_matches']}")
    print(f"expired: {marker['expired']}")
    print(f"next_recommended_action: {decision['next_recommended_action']}")
    print(f"production_effect: {payload['production_effect']}")
    print(f"manual_review_only: {payload['manual_review_only']}")
    print(f"approval_gate_only: {payload['approval_gate_only']}")
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
    print(f"Approval Gate JSON: {outputs['approval_gate_json']['path']}")
    print(f"Approval Gate Markdown: {outputs['approval_gate_markdown']['path']}")
    print(f"Latest JSON: {outputs['latest_json']['path']}")
    print(f"Latest Markdown: {outputs['latest_markdown']['path']}")
    print(f"Run Log: {outputs['run_log']['path']}")
    if should_fail_cli(payload):
        raise SystemExit(1)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
