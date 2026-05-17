from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.reports.weight_candidate_evaluation import (  # noqa: E402
    DEFAULT_PARAMETER_GOVERNANCE_PATH,
    DEFAULT_PRODUCTION_PROFILE_PATH,
    DEFAULT_SHADOW_PROFILES_PATH,
    DEFAULT_WEIGHT_CANDIDATE_EVALUATION_POLICY_PATH,
    write_weight_candidate_evaluation_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build observe-only weight candidate evaluation reports."
    )
    parser.add_argument("--date", required=True, help="Evaluation date in YYYY-MM-DD format.")
    parser.add_argument(
        "--reports-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory containing candidate, paper quality, shadow impact, and replay JSON files.",
    )
    parser.add_argument(
        "--policy-path",
        default=str(DEFAULT_WEIGHT_CANDIDATE_EVALUATION_POLICY_PATH),
        help="Weight candidate evaluation policy YAML path.",
    )
    parser.add_argument("--weight-adjustment-candidates-json", help="Candidate JSON path.")
    parser.add_argument("--paper-signal-quality-json", help="Paper quality JSON path.")
    parser.add_argument("--shadow-parameter-impact-json", help="Shadow impact JSON path.")
    parser.add_argument("--replay-json", help="Optional existing paper_trading_replay JSON path.")
    parser.add_argument(
        "--continuous-replay-summary-json",
        help="Optional existing continuous replay summary JSON path.",
    )
    parser.add_argument("--daily-decision-summary-json", help="Optional daily summary JSON path.")
    parser.add_argument(
        "--parameter-governance-path",
        default=str(DEFAULT_PARAMETER_GOVERNANCE_PATH),
        help="Parameter governance manifest path.",
    )
    parser.add_argument(
        "--production-profile-path",
        default=str(DEFAULT_PRODUCTION_PROFILE_PATH),
        help="Current production weight profile path.",
    )
    parser.add_argument(
        "--shadow-profiles-path",
        default=str(DEFAULT_SHADOW_PROFILES_PATH),
        help="Existing shadow weight profiles path.",
    )
    parser.add_argument("--output-json-path", help="Output JSON path.")
    parser.add_argument("--output-md-path", help="Output Markdown path.")
    parser.add_argument(
        "--selected-window-days",
        type=int,
        default=30,
        choices=(7, 14, 30),
        help="Window used for top-level evaluation status.",
    )
    args = parser.parse_args()

    payload = write_weight_candidate_evaluation_report(
        as_of=date.fromisoformat(args.date),
        reports_dir=Path(args.reports_dir),
        policy_path=Path(args.policy_path),
        weight_adjustment_candidates_path=(
            Path(args.weight_adjustment_candidates_json)
            if args.weight_adjustment_candidates_json
            else None
        ),
        paper_signal_quality_path=(
            Path(args.paper_signal_quality_json) if args.paper_signal_quality_json else None
        ),
        shadow_parameter_impact_path=(
            Path(args.shadow_parameter_impact_json) if args.shadow_parameter_impact_json else None
        ),
        replay_json_path=Path(args.replay_json) if args.replay_json else None,
        continuous_replay_summary_path=(
            Path(args.continuous_replay_summary_json)
            if args.continuous_replay_summary_json
            else None
        ),
        parameter_governance_path=Path(args.parameter_governance_path),
        production_profile_path=Path(args.production_profile_path),
        shadow_profiles_path=Path(args.shadow_profiles_path),
        daily_decision_summary_path=(
            Path(args.daily_decision_summary_json) if args.daily_decision_summary_json else None
        ),
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
        selected_window_days=args.selected_window_days,
    )
    summary = payload["summary"]
    outputs = payload["outputs"]
    print(f"Weight candidate evaluation：{payload['evaluation_status']}")
    print(f"candidate_count：{summary['candidate_count']}")
    print(f"evaluable_candidate_count：{summary['evaluable_candidate_count']}")
    print(f"top_candidate_id：{summary['top_candidate_id']}")
    print(f"main_blocked_by：{summary['main_blocked_by']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"JSON：{outputs['json']}")
    print(f"Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
