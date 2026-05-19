from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.reports.daily_shadow_weight_iteration import (  # noqa: E402
    DEFAULT_DAILY_SHADOW_WEIGHT_ITERATION_POLICY_PATH,
    DEFAULT_PRODUCTION_PROFILE_PATH,
    write_daily_shadow_weight_iteration_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build shadow-only daily weight iteration candidates and state."
    )
    parser.add_argument("--date", required=True, help="Iteration date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(REPO_ROOT / "data"),
        help="Data root for shadow state artifacts.",
    )
    parser.add_argument(
        "--reports-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory containing existing TRADING-015/016/017/018/018A artifacts.",
    )
    parser.add_argument(
        "--policy-path",
        default=str(DEFAULT_DAILY_SHADOW_WEIGHT_ITERATION_POLICY_PATH),
        help="Shadow weight iteration policy YAML path.",
    )
    parser.add_argument(
        "--production-profile-path",
        default=str(DEFAULT_PRODUCTION_PROFILE_PATH),
        help="Production profile path used only for initial shadow snapshot copy.",
    )
    parser.add_argument("--scheduler-dry-run-json", help="Optional TRADING-018A JSON path.")
    parser.add_argument("--output-json-path", help="Output candidate JSON path.")
    parser.add_argument("--output-md-path", help="Output candidate Markdown path.")
    parser.add_argument(
        "--observe-only",
        action="store_true",
        default=True,
        help="Accepted for scheduler clarity; shadow mode is always observe-only from production.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate candidate/log artifacts without updating current shadow state.",
    )
    parser.add_argument(
        "--force-no-update",
        action="store_true",
        help="Force a NO_UPDATE candidate even when inputs would allow UPDATE.",
    )
    args = parser.parse_args()

    payload = write_daily_shadow_weight_iteration_report(
        as_of=date.fromisoformat(args.date),
        reports_dir=Path(args.reports_dir),
        data_root=Path(args.data_root),
        policy_path=Path(args.policy_path),
        production_profile_path=Path(args.production_profile_path),
        scheduler_dry_run_path=(
            Path(args.scheduler_dry_run_json) if args.scheduler_dry_run_json else None
        ),
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
        force_no_update=args.force_no_update,
        dry_run=args.dry_run,
    )
    outputs = payload["outputs"]
    run_log = payload.get("run_log", {})
    print(f"Shadow 权重迭代决策：{payload['decision']}")
    print(f"决策原因：{payload['decision_reason']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"current_shadow_weights 已更新：{run_log.get('current_state_updated', False)}")
    print(f"Candidate JSON：{outputs['candidate_json']}")
    print(f"Candidate Markdown：{outputs['candidate_markdown']}")
    print(f"Current shadow weights：{outputs['current_shadow_weights']}")
    print(f"Run log JSON：{outputs['run_log_json']}")


if __name__ == "__main__":
    main()
