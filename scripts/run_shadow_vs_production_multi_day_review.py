from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.reports.daily_shadow_vs_production_multi_day_review import (  # noqa: E402
    write_shadow_vs_production_multi_day_review_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a read-only multi-day shadow vs production review report."
    )
    parser.add_argument("--date", required=True, help="Review date in YYYY-MM-DD format.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Calendar-day lookback window for existing comparison artifacts.",
    )
    parser.add_argument(
        "--data-root",
        default=str(REPO_ROOT / "data"),
        help="Data root containing weight iteration comparison artifacts.",
    )
    parser.add_argument("--output-json-path", help="Output review JSON path.")
    parser.add_argument("--output-md-path", help="Output review Markdown path.")
    args = parser.parse_args()

    payload = write_shadow_vs_production_multi_day_review_report(
        as_of=date.fromisoformat(args.date),
        lookback_days=args.lookback_days,
        data_root=Path(args.data_root),
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["outputs"]
    print(f"Shadow vs Production multi-day review：{payload['review_decision']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"lookback_days：{payload['lookback_days']}")
    print(f"available_comparison_days：{payload['available_comparison_days']}")
    print(f"average_score_delta：{payload['average_score_delta']}")
    print(f"decision_difference_count：{payload['decision_difference_count']}")
    print(f"Review JSON：{outputs['json']}")
    print(f"Review Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
