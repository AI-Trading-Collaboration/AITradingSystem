from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.reports.paper_signal_quality import (  # noqa: E402
    DEFAULT_PAPER_SIGNAL_QUALITY_POLICY_PATH,
    write_paper_signal_quality_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build observe-only paper signal quality evaluation reports."
    )
    parser.add_argument("--date", required=True, help="Evaluation date in YYYY-MM-DD format.")
    parser.add_argument(
        "--reports-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory containing paper summary and order intent candidate JSON files.",
    )
    parser.add_argument(
        "--policy-path",
        default=str(DEFAULT_PAPER_SIGNAL_QUALITY_POLICY_PATH),
        help="Paper signal quality policy YAML path.",
    )
    parser.add_argument(
        "--replay-json",
        help="Optional existing paper_trading_replay_START_END.json path.",
    )
    parser.add_argument("--output-json-path", help="Output JSON path.")
    parser.add_argument("--output-md-path", help="Output Markdown path.")
    parser.add_argument(
        "--selected-window-days",
        type=int,
        default=30,
        choices=(7, 14, 30),
        help="Window used for top-level summary and aggregations.",
    )
    args = parser.parse_args()

    payload = write_paper_signal_quality_report(
        as_of=date.fromisoformat(args.date),
        reports_dir=Path(args.reports_dir),
        policy_path=Path(args.policy_path),
        replay_json_path=Path(args.replay_json) if args.replay_json else None,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
        selected_window_days=args.selected_window_days,
    )
    summary = payload["summary"]
    outputs = payload["outputs"]
    print(f"Paper signal quality：{payload['evaluation_status']}")
    print(f"sample_count：{summary['sample_count']}")
    print(f"candidate_count：{summary['candidate_count']}")
    print(f"filled_count：{summary['filled_count']}")
    print(f"primary_blocked_by：{summary['primary_blocked_by']}")
    print(f"JSON：{outputs['json']}")
    print(f"Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
