from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.shadow_promotion_apply import (  # noqa: E402
    DANGER_FLAG,
    write_shadow_promotion_apply_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Explicitly apply an approved shadow promotion to a production profile."
    )
    parser.add_argument("--date", required=True, help="Apply date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(REPO_ROOT / "data"),
        help="Data root containing promotion artifacts.",
    )
    parser.add_argument(
        "--preflight-file",
        required=True,
        help="TRADING-018E1 apply preflight JSON path.",
    )
    parser.add_argument(
        "--apply-approval-file",
        required=True,
        help="TRADING-018E2 apply approval JSON path.",
    )
    parser.add_argument(
        "--target-profile",
        required=True,
        help="Target production profile path. This command may update only its weights.",
    )
    parser.add_argument("--proposal-file", help="TRADING-018D promotion proposal JSON path.")
    parser.add_argument(
        "--expected-target-profile-sha256",
        help="Optional explicit sha256 expected for the target profile before apply.",
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
        help="Block apply when non-blocking approval warnings are present.",
    )
    parser.add_argument(
        "--output-json-path",
        help="Output apply result JSON path.",
    )
    parser.add_argument(
        "--output-md-path",
        help="Output apply result Markdown path.",
    )
    parser.add_argument(
        "--i-understand-this-writes-production",
        action="store_true",
        help="Required explicit acknowledgement that this command writes production weights.",
    )
    args = parser.parse_args()

    payload = write_shadow_promotion_apply_report(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        preflight_file=Path(args.preflight_file),
        apply_approval_file=Path(args.apply_approval_file),
        target_profile_path=Path(args.target_profile),
        proposal_file=Path(args.proposal_file) if args.proposal_file else None,
        expected_target_profile_sha256=args.expected_target_profile_sha256,
        danger_flag_provided=args.i_understand_this_writes_production,
        write_mode=args.write_mode,
        fail_on_warning=args.fail_on_warning,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["outputs"]
    diff = payload["diff_applied"]
    rollback = payload["rollback"]
    print(f"Shadow promotion apply：{payload['apply_decision']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"promotion_executed：{payload['promotion_executed']}")
    print(f"apply_executed：{payload['apply_executed']}")
    print(f"safe_for_scheduler：{payload['safe_for_scheduler']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"changed_weight_keys：{', '.join(diff.get('changed_weight_keys', [])) or 'none'}")
    print(f"rollback_snapshot：{rollback.get('snapshot_path', '')}")
    print(f"Apply JSON：{outputs['json']}")
    print(f"Apply Markdown：{outputs['markdown']}")
    if not args.i_understand_this_writes_production:
        print(f"Missing required flag：{DANGER_FLAG}")


if __name__ == "__main__":
    main()
