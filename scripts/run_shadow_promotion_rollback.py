from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.shadow_promotion_rollback import (  # noqa: E402
    DANGER_FLAG,
    write_shadow_promotion_rollback_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Explicitly roll back an approved shadow promotion apply."
    )
    parser.add_argument("--date", required=True, help="Rollback date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(REPO_ROOT / "data"),
        help="Data root containing promotion artifacts.",
    )
    parser.add_argument(
        "--apply-result-file",
        required=True,
        help="TRADING-018E2 apply result JSON path.",
    )
    parser.add_argument(
        "--rollback-approval-file",
        required=True,
        help="TRADING-018E3 rollback approval JSON path.",
    )
    parser.add_argument(
        "--target-profile",
        required=True,
        help="Target production profile path. This command may restore only its weights.",
    )
    parser.add_argument(
        "--expected-current-profile-sha256",
        help="Optional explicit sha256 expected for the target profile before rollback.",
    )
    parser.add_argument(
        "--write-mode",
        default="atomic",
        choices=("atomic",),
        help="Production profile write mode. Only atomic is supported.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Block rollback when non-blocking approval warnings are present.",
    )
    parser.add_argument(
        "--output-json-path",
        help="Output rollback result JSON path.",
    )
    parser.add_argument(
        "--output-md-path",
        help="Output rollback result Markdown path.",
    )
    parser.add_argument(
        "--i-understand-this-rolls-back-production",
        action="store_true",
        help="Required explicit acknowledgement that this command rolls back production weights.",
    )
    args = parser.parse_args()

    payload = write_shadow_promotion_rollback_report(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        apply_result_file=Path(args.apply_result_file),
        rollback_approval_file=Path(args.rollback_approval_file),
        target_profile_path=Path(args.target_profile),
        expected_current_profile_sha256=args.expected_current_profile_sha256,
        danger_flag_provided=args.i_understand_this_rolls_back_production,
        write_mode=args.write_mode,
        fail_on_warning=args.fail_on_warning,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["outputs"]
    rollback = payload["rollback_applied"]
    current_snapshot = payload["current_snapshot"]
    print(f"Shadow promotion rollback：{payload['rollback_decision']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"rollback_executed：{payload['rollback_executed']}")
    print(f"safe_for_scheduler：{payload['safe_for_scheduler']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print("changed_weight_keys：" f"{', '.join(rollback.get('changed_weight_keys', [])) or 'none'}")
    print(f"current_snapshot：{current_snapshot.get('snapshot_path', '')}")
    print(f"Rollback JSON：{outputs['json']}")
    print(f"Rollback Markdown：{outputs['markdown']}")
    if not args.i_understand_this_rolls_back_production:
        print(f"Missing required flag：{DANGER_FLAG}")


if __name__ == "__main__":
    main()
