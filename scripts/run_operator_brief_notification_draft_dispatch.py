from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.operator_brief_notification_draft_dispatch import (  # noqa: E402
    DRAFT_READY,
    should_fail_cli,
    write_operator_brief_notification_draft_dispatch,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate read-only operator brief notification draft dispatch artifact."
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Draft dispatch date in YYYY-MM-DD format. Defaults to today's local date.",
    )
    parser.add_argument(
        "--data-root",
        default="data",
        help="Data root containing existing TRADING-032/TRADING-033 artifacts.",
    )
    parser.add_argument(
        "--input-approval-gate",
        default=None,
        help="Explicit TRADING-033 approval gate JSON path.",
    )
    parser.add_argument(
        "--input-dispatch-preview",
        default=None,
        help="Explicit TRADING-032 dispatch preview JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for draft dispatch artifacts. Must stay inside repo root.",
    )
    args = parser.parse_args()

    try:
        as_of = date.fromisoformat(args.date) if args.date else date.today()
        payload = write_operator_brief_notification_draft_dispatch(
            as_of=as_of,
            data_root=Path(args.data_root),
            input_approval_gate_file=_optional_path(args.input_approval_gate),
            input_dispatch_preview_file=_optional_path(args.input_dispatch_preview),
            output_dir=_optional_path(args.output_dir),
        )
    except Exception as exc:
        print("FAIL: operator brief notification draft dispatch failed")
        print(f"error: {exc}")
        raise SystemExit(1) from exc

    decision = payload["decision"]
    gate = payload["approval_gate_summary"]
    draft = payload["draft"]
    hashes = payload["hashes"]
    outputs = payload["output_artifacts"]
    status = decision["final_status"]
    if status == DRAFT_READY:
        print("PASS: operator brief notification draft dispatch generated")
    else:
        print(
            "LIMITED: operator brief notification draft dispatch generated " "with non-ready status"
        )
    print(f"final_status: {status}")
    print(f"ready_for_actual_dispatch: {decision['ready_for_actual_dispatch']}")
    print(f"human_action_required: {decision['human_action_required']}")
    print(f"approval_gate_status: {gate['approval_gate_status']}")
    print(f"allowed_to_enter_dispatch: {gate['allowed_to_enter_dispatch']}")
    print(f"channel_count: {draft['channel_count']}")
    print(f"draft_ready_channel_count: {draft['draft_ready_channel_count']}")
    print(f"dispatch_preview_hash: {hashes['dispatch_preview_hash']}")
    print(f"draft_hash: {hashes['draft_hash']}")
    print(f"next_recommended_action: {decision['next_recommended_action']}")
    print(f"production_effect: {payload['production_effect']}")
    print(f"manual_review_only: {payload['manual_review_only']}")
    print(f"draft_dispatch_only: {payload['draft_dispatch_only']}")
    print(f"external_side_effects: {payload['external_side_effects']}")
    print(f"network_access_required: {payload['network_access_required']}")
    print(f"secrets_required: {payload['secrets_required']}")
    print(f"email_sent: {payload['email_sent']}")
    print(f"gmail_draft_created: {payload['gmail_draft_created']}")
    print(f"smtp_called: {payload['smtp_called']}")
    print(f"webhook_called: {payload['webhook_called']}")
    print(f"Draft Dispatch JSON: {outputs['draft_dispatch_json']['path']}")
    print(f"Draft Dispatch Markdown: {outputs['draft_dispatch_markdown']['path']}")
    print(f"Latest JSON: {outputs['latest_json']['path']}")
    print(f"Latest Markdown: {outputs['latest_markdown']['path']}")
    print(f"Run Log: {outputs['run_log']['path']}")
    if should_fail_cli(payload):
        raise SystemExit(1)


def _optional_path(value: str | None) -> Path | None:
    return None if value in (None, "") else Path(value)


if __name__ == "__main__":
    main()
