from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.reports.paperbroker_fill_model_calibration import (  # noqa: E402
    DEFAULT_PAPERBROKER_FILL_MODEL_CALIBRATION_POLICY_PATH,
    write_paperbroker_fill_model_calibration_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build diagnostic-only PaperBroker fill model calibration reports."
    )
    parser.add_argument("--date", required=True, help="Calibration date in YYYY-MM-DD format.")
    parser.add_argument(
        "--reports-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory containing comparison, replay, and paper signal quality JSON files.",
    )
    parser.add_argument(
        "--policy-path",
        default=str(DEFAULT_PAPERBROKER_FILL_MODEL_CALIBRATION_POLICY_PATH),
        help="PaperBroker fill model calibration policy YAML path.",
    )
    parser.add_argument(
        "--max-comparisons",
        type=int,
        default=None,
        help="Maximum recent comparison JSON files to read. Defaults to policy threshold.",
    )
    parser.add_argument(
        "--replay-json",
        help="Optional existing paper_trading_replay_START_END.json path.",
    )
    parser.add_argument(
        "--paper-signal-quality-json",
        help="Optional existing paper_signal_quality_YYYY-MM-DD.json path.",
    )
    parser.add_argument("--output-json-path", help="Output JSON path.")
    parser.add_argument("--output-md-path", help="Output Markdown path.")
    args = parser.parse_args()

    payload = write_paperbroker_fill_model_calibration_report(
        as_of=date.fromisoformat(args.date),
        reports_dir=Path(args.reports_dir),
        policy_path=Path(args.policy_path),
        max_comparisons=args.max_comparisons,
        replay_json_path=Path(args.replay_json) if args.replay_json else None,
        paper_signal_quality_json_path=(
            Path(args.paper_signal_quality_json) if args.paper_signal_quality_json else None
        ),
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    summary = payload["summary"]
    outputs = payload["outputs"]
    print(f"PaperBroker fill model calibration：{payload['calibration_status']}")
    print(f"calibration_mode={payload['calibration_mode']}")
    print(f"production_effect={payload['production_effect']}")
    print(f"comparison_count：{summary['comparison_count']}")
    print(f"lifecycle_match_ratio：{summary['lifecycle_match_ratio']:.2%}")
    print(f"fill_tested={str(payload['fill_tested']).lower()}")
    print(f"JSON：{outputs['json']}")
    print(f"Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
