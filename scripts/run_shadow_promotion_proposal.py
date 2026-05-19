from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.shadow_promotion_proposal import (  # noqa: E402
    DEFAULT_POLICY_PATH,
    DEFAULT_PRODUCTION_PROFILE_PATH,
    write_shadow_promotion_proposal_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a manual-only shadow-to-production promotion proposal."
    )
    parser.add_argument("--date", required=True, help="Proposal date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(REPO_ROOT / "data"),
        help="Data root containing shadow, comparison, and promotion artifacts.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Calendar-day lookback window for existing comparison artifacts.",
    )
    parser.add_argument(
        "--production-profile-path",
        default=str(DEFAULT_PRODUCTION_PROFILE_PATH),
        help="Production weight profile YAML path. The script only reads this file.",
    )
    parser.add_argument(
        "--policy-path",
        default=str(DEFAULT_POLICY_PATH),
        help="Shadow promotion proposal policy YAML path.",
    )
    parser.add_argument("--current-shadow-weights-path", help="current_shadow_weights JSON path.")
    parser.add_argument("--latest-multi-day-review-path", help="TRADING-018C2 review JSON path.")
    parser.add_argument("--output-json-path", help="Output proposal JSON path.")
    parser.add_argument("--output-md-path", help="Output proposal Markdown path.")
    parser.add_argument(
        "--force-no-proposal",
        action="store_true",
        help="Force CONTINUE_OBSERVATION even when all proposal checks pass.",
    )
    args = parser.parse_args()

    payload = write_shadow_promotion_proposal_report(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        lookback_days=args.lookback_days,
        production_profile_path=Path(args.production_profile_path),
        policy_path=Path(args.policy_path),
        current_shadow_weights_path=(
            Path(args.current_shadow_weights_path) if args.current_shadow_weights_path else None
        ),
        latest_multi_day_review_path=(
            Path(args.latest_multi_day_review_path) if args.latest_multi_day_review_path else None
        ),
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
        force_no_proposal=args.force_no_proposal,
    )
    outputs = payload["outputs"]
    impact = payload["impact_summary"]
    print(f"Shadow promotion proposal：{payload['proposal_decision']}")
    print(f"promotion_proposed：{payload['promotion_proposed']}")
    print(f"promotion_executed：{payload['promotion_executed']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"available_comparison_days：{impact['available_comparison_days']}")
    print(f"average_score_delta：{impact['expected_score_delta']}")
    print(f"Proposal JSON：{outputs['json']}")
    print(f"Proposal Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
