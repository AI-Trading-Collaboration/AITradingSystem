from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.reports.daily_shadow_vs_production_comparison import (  # noqa: E402
    DEFAULT_PRODUCTION_PROFILE_PATH,
    DEFAULT_SCORING_RULES_PATH,
    write_daily_shadow_vs_production_comparison_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build an offline shadow vs production weight comparison report."
    )
    parser.add_argument("--date", required=True, help="Comparison date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(REPO_ROOT / "data"),
        help="Data root for decision snapshots and shadow state artifacts.",
    )
    parser.add_argument(
        "--reports-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory containing existing daily scoring / feedback artifacts.",
    )
    parser.add_argument(
        "--production-profile-path",
        default=str(DEFAULT_PRODUCTION_PROFILE_PATH),
        help="Production weight profile YAML path.",
    )
    parser.add_argument(
        "--scoring-rules-path",
        default=str(DEFAULT_SCORING_RULES_PATH),
        help="Scoring rules YAML path used for position bands.",
    )
    parser.add_argument("--current-shadow-weights-path", help="current_shadow_weights JSON path.")
    parser.add_argument(
        "--shadow-iteration-candidate-path",
        help="TRADING-018B candidate JSON path.",
    )
    parser.add_argument("--decision-snapshot-path", help="Decision snapshot JSON path.")
    parser.add_argument("--output-json-path", help="Output comparison JSON path.")
    parser.add_argument("--output-md-path", help="Output comparison Markdown path.")
    args = parser.parse_args()

    payload = write_daily_shadow_vs_production_comparison_report(
        as_of=date.fromisoformat(args.date),
        reports_dir=Path(args.reports_dir),
        data_root=Path(args.data_root),
        production_profile_path=Path(args.production_profile_path),
        scoring_rules_path=Path(args.scoring_rules_path),
        current_shadow_weights_path=(
            Path(args.current_shadow_weights_path) if args.current_shadow_weights_path else None
        ),
        shadow_iteration_candidate_path=(
            Path(args.shadow_iteration_candidate_path)
            if args.shadow_iteration_candidate_path
            else None
        ),
        decision_snapshot_path=(
            Path(args.decision_snapshot_path) if args.decision_snapshot_path else None
        ),
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["outputs"]
    difference = payload.get("difference", {})
    print(f"Shadow vs Production comparison：{payload['comparison_status']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"score_delta：{difference.get('score_delta')}")
    print(f"decision_changed：{difference.get('decision_changed')}")
    print(f"main_reason：{difference.get('main_reason')}")
    print(f"Comparison JSON：{outputs['json']}")
    print(f"Comparison Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
