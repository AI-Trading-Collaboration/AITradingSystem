from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.reports.daily_weight_adjustment import (  # noqa: E402
    write_daily_weight_adjustment_summary_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build observe-only daily weight adjustment summary reports."
    )
    parser.add_argument("--date", required=True, help="Summary date in YYYY-MM-DD format.")
    parser.add_argument(
        "--reports-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory containing existing weight adjustment artifacts.",
    )
    parser.add_argument(
        "--weight-adjustment-candidates-json",
        help="Existing weight adjustment candidates JSON path.",
    )
    parser.add_argument(
        "--weight-adjustment-candidates-md",
        help="Existing weight adjustment candidates Markdown path.",
    )
    parser.add_argument(
        "--weight-candidate-evaluation-json",
        help="Existing weight candidate evaluation JSON path.",
    )
    parser.add_argument(
        "--weight-candidate-evaluation-md",
        help="Existing weight candidate evaluation Markdown path.",
    )
    parser.add_argument(
        "--weight-promotion-gate-json",
        help="Existing weight promotion gate JSON path.",
    )
    parser.add_argument(
        "--weight-promotion-gate-md",
        help="Existing weight promotion gate Markdown path.",
    )
    parser.add_argument("--output-json-path", help="Output summary JSON path.")
    parser.add_argument("--output-md-path", help="Output summary Markdown path.")
    args = parser.parse_args()

    payload = write_daily_weight_adjustment_summary_report(
        as_of=date.fromisoformat(args.date),
        reports_dir=Path(args.reports_dir),
        weight_adjustment_candidates_path=(
            Path(args.weight_adjustment_candidates_json)
            if args.weight_adjustment_candidates_json
            else None
        ),
        weight_adjustment_candidates_md_path=(
            Path(args.weight_adjustment_candidates_md)
            if args.weight_adjustment_candidates_md
            else None
        ),
        weight_candidate_evaluation_path=(
            Path(args.weight_candidate_evaluation_json)
            if args.weight_candidate_evaluation_json
            else None
        ),
        weight_candidate_evaluation_md_path=(
            Path(args.weight_candidate_evaluation_md)
            if args.weight_candidate_evaluation_md
            else None
        ),
        weight_promotion_gate_path=(
            Path(args.weight_promotion_gate_json) if args.weight_promotion_gate_json else None
        ),
        weight_promotion_gate_md_path=(
            Path(args.weight_promotion_gate_md) if args.weight_promotion_gate_md else None
        ),
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["outputs"]
    print(f"Daily weight adjustment status：{payload['status']}")
    print(f"candidate_count：{payload['candidate_count']}")
    print(f"evaluation_status：{payload['evaluation_status']}")
    print(f"promotion_gate_status：{payload['promotion_gate_status']}")
    print(f"ready_for_manual_review_count：{payload['ready_for_manual_review_count']}")
    print(f"main_blocked_by：{payload['main_blocked_by']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"JSON：{outputs['json']}")
    print(f"Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
