from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.parameter_governance_daily_digest import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    DEFAULT_LOOKBACK_DAYS,
    write_parameter_governance_daily_digest,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a read-only parameter governance daily digest."
    )
    parser.add_argument("--date", required=True, help="Digest date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Data root containing existing parameter governance artifacts.",
    )
    parser.add_argument(
        "--governance-summary-file",
        help="Explicit parameter_governance_summary_YYYY-MM-DD.json path to read only.",
    )
    parser.add_argument(
        "--web-view-metadata-file",
        help="Optional parameter_governance_web_view_YYYY-MM-DD.json metadata path.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Calendar-day lookback window for latest optional governance artifacts.",
    )
    parser.add_argument(
        "--fail-on-safety-anomaly",
        action="store_true",
        help="Write artifacts, then exit non-zero if a safety anomaly is detected.",
    )
    parser.add_argument("--output-json-path", help="Output daily digest JSON path.")
    parser.add_argument("--output-md-path", help="Output daily digest Markdown path.")
    args = parser.parse_args()

    payload = write_parameter_governance_daily_digest(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        governance_summary_file=(
            Path(args.governance_summary_file) if args.governance_summary_file else None
        ),
        web_view_metadata_file=(
            Path(args.web_view_metadata_file) if args.web_view_metadata_file else None
        ),
        lookback_days=args.lookback_days,
        fail_on_safety_anomaly=args.fail_on_safety_anomaly,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["output_artifacts"]
    readout = payload["daily_readout"]
    snapshot = payload["governance_snapshot"]
    alerts = payload["alerts"]
    print(f"Parameter Governance Daily Digest：{payload['digest_status']}")
    print(f"summary_level：{payload['summary_level']}")
    print(f"headline：{payload['headline']}")
    print(f"governance_state：{snapshot['governance_state']}")
    print(f"action_required：{snapshot['action_required']}")
    print(f"action_level：{snapshot['action_level']}")
    print(f"has_pending_apply：{readout['has_pending_apply']}")
    print(f"has_pending_rollback：{readout['has_pending_rollback']}")
    print(f"has_safety_anomaly：{readout['has_safety_anomaly']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"digest_only：{payload['digest_only']}")
    print(f"governance_only：{payload['governance_only']}")
    print(f"apply_executed_by_digest：{payload['apply_executed_by_digest']}")
    print(f"rollback_executed_by_digest：{payload['rollback_executed_by_digest']}")
    print(f"safe_for_scheduler：{payload['safe_for_scheduler']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"critical_alerts：{len(alerts.get('critical', []))}")
    print(f"warnings：{len(alerts.get('warnings', []))}")
    print(f"Digest JSON：{outputs['json']['path']}")
    print(f"Digest Markdown：{outputs['markdown']['path']}")


if __name__ == "__main__":
    main()
