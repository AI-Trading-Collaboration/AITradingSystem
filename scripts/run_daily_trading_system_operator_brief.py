from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.daily_trading_system_operator_brief import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    DEFAULT_LOOKBACK_DAYS,
    write_daily_trading_system_operator_brief,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a read-only daily trading system operator brief."
    )
    parser.add_argument("--date", required=True, help="Brief date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Data root containing existing operator and governance artifacts.",
    )
    parser.add_argument(
        "--parameter-governance-digest-file",
        help="Explicit parameter_governance_daily_digest_YYYY-MM-DD.json path to read only.",
    )
    parser.add_argument(
        "--pipeline-health-summary-file",
        help="Explicit pipeline_health_summary_YYYY-MM-DD.json path to read only.",
    )
    parser.add_argument(
        "--data-freshness-summary-file",
        help="Explicit data_freshness_summary_YYYY-MM-DD.json path to read only.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Calendar-day lookback window for latest artifacts.",
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Write artifacts, then exit non-zero if critical alerts are present.",
    )
    parser.add_argument(
        "--include-optional-artifacts",
        action="store_true",
        help="Also scan optional weight/backtest artifact families where supported.",
    )
    parser.add_argument("--output-json-path", help="Output operator brief JSON path.")
    parser.add_argument("--output-md-path", help="Output operator brief Markdown path.")
    args = parser.parse_args()

    payload = write_daily_trading_system_operator_brief(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        parameter_governance_digest_file=(
            Path(args.parameter_governance_digest_file)
            if args.parameter_governance_digest_file
            else None
        ),
        pipeline_health_summary_file=(
            Path(args.pipeline_health_summary_file) if args.pipeline_health_summary_file else None
        ),
        data_freshness_summary_file=(
            Path(args.data_freshness_summary_file) if args.data_freshness_summary_file else None
        ),
        lookback_days=args.lookback_days,
        fail_on_critical=args.fail_on_critical,
        include_optional_artifacts=args.include_optional_artifacts,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["output_artifacts"]
    snapshot = payload["system_snapshot"]
    governance = payload["parameter_governance"]
    pipeline = payload["pipeline_health"]
    freshness = payload["data_freshness"]
    alerts = payload["alerts"]

    print(f"Daily Trading System Operator Brief：{payload['brief_status']}")
    print(f"summary_level：{payload['summary_level']}")
    print(f"headline：{payload['headline']}")
    print(f"can_trust_outputs_today：{snapshot['can_trust_outputs_today']}")
    print(f"manual_action_required：{snapshot['manual_action_required']}")
    print(f"parameter_governance.digest_status：{governance['digest_status']}")
    print(f"pipeline_health.status：{pipeline['status']}")
    print(f"pipeline_health.health_status：{pipeline.get('health_status')}")
    print(f"data_freshness.status：{freshness['status']}")
    print(f"data_freshness.freshness_status：{freshness.get('freshness_status')}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"operator_brief_only：{payload['operator_brief_only']}")
    print(f"read_only：{payload['read_only']}")
    print(f"apply_executed_by_operator_brief：{payload['apply_executed_by_operator_brief']}")
    print(f"rollback_executed_by_operator_brief：{payload['rollback_executed_by_operator_brief']}")
    print(f"safe_for_scheduler：{payload['safe_for_scheduler']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"critical_alerts：{len(alerts.get('critical', []))}")
    print(f"warnings：{len(alerts.get('warnings', []))}")
    print(f"Operator Brief JSON：{outputs['json']['path']}")
    print(f"Operator Brief Markdown：{outputs['markdown']['path']}")


if __name__ == "__main__":
    main()
