from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.data_freshness_summary import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    DEFAULT_FRESHNESS_DAYS,
    DEFAULT_LOOKBACK_DAYS,
    write_data_freshness_summary,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a read-only data freshness summary from existing artifacts."
    )
    parser.add_argument("--date", required=True, help="Summary date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Data root containing existing source artifacts.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Calendar-day lookback window for latest artifacts.",
    )
    parser.add_argument(
        "--freshness-days",
        type=int,
        default=DEFAULT_FRESHNESS_DAYS,
        help="Default freshness threshold for sources without registry overrides.",
    )
    parser.add_argument(
        "--market-date",
        help="Latest market data date in YYYY-MM-DD format, used as freshness reference.",
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Write artifacts, then exit non-zero if freshness_status is CRITICAL.",
    )
    parser.add_argument(
        "--include-optional-sources",
        action="store_true",
        help="Also scan optional market, backtest, cache, and support sources.",
    )
    parser.add_argument("--output-json-path", help="Output data freshness JSON path.")
    parser.add_argument("--output-md-path", help="Output data freshness Markdown path.")
    args = parser.parse_args()

    payload = write_data_freshness_summary(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        lookback_days=args.lookback_days,
        freshness_days=args.freshness_days,
        market_date=date.fromisoformat(args.market_date) if args.market_date else None,
        fail_on_critical=args.fail_on_critical,
        include_optional_sources=args.include_optional_sources,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    coverage = payload["coverage"]
    outputs = payload["output_artifacts"]
    alerts = payload["alerts"]

    print(f"Data Freshness Summary：{payload['freshness_status']}")
    print(f"summary_level：{payload['summary_level']}")
    print(f"headline：{payload['headline']}")
    print(f"registered_sources：{coverage['registered_sources']}")
    print(f"required_sources：{coverage['required_sources']}")
    print(f"available_sources：{coverage['available_sources']}")
    print(f"missing_required_sources：{coverage['missing_required_sources']}")
    print(f"stale_required_sources：{coverage['stale_required_sources']}")
    print(f"critical_sources：{coverage['critical_sources']}")
    print(f"warning_sources：{coverage['warning_sources']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"data_freshness_only：{payload['data_freshness_only']}")
    print(f"read_only：{payload['read_only']}")
    print("data_downloaded_by_freshness_check：" f"{payload['data_downloaded_by_freshness_check']}")
    print(
        "pipelines_executed_by_freshness_check："
        f"{payload['pipelines_executed_by_freshness_check']}"
    )
    print(f"apply_executed_by_freshness_check：{payload['apply_executed_by_freshness_check']}")
    print(
        "rollback_executed_by_freshness_check："
        f"{payload['rollback_executed_by_freshness_check']}"
    )
    print(f"safe_for_scheduler：{payload['safe_for_scheduler']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"critical_alerts：{len(alerts.get('critical', []))}")
    print(f"warnings：{len(alerts.get('warnings', []))}")
    print(f"Data Freshness JSON：{outputs['json']['path']}")
    print(f"Data Freshness Markdown：{outputs['markdown']['path']}")


if __name__ == "__main__":
    main()
