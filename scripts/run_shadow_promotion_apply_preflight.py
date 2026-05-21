from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.shadow_promotion_apply_preflight import (  # noqa: E402
    DEFAULT_PRODUCTION_PROFILE_PATH,
    DEFAULT_TARGET_PROFILE_NAME,
    write_shadow_promotion_apply_preflight_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a read-only approved shadow promotion apply preflight report."
    )
    parser.add_argument("--date", required=True, help="Preflight date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(REPO_ROOT / "data"),
        help="Data root containing promotion and shadow artifacts.",
    )
    parser.add_argument("--approval-file", help="Manual approval artifact JSON path.")
    parser.add_argument("--proposal-file", help="TRADING-018D promotion proposal JSON path.")
    parser.add_argument(
        "--production-profile",
        default=str(DEFAULT_PRODUCTION_PROFILE_PATH),
        help="Production profile path. The script only reads this file.",
    )
    parser.add_argument(
        "--target-profile-name",
        default=DEFAULT_TARGET_PROFILE_NAME,
        help="Expected target profile name declared by the approval artifact.",
    )
    parser.add_argument("--current-shadow-weights-path", help="current_shadow_weights JSON path.")
    parser.add_argument("--output-json-path", help="Output preflight JSON path.")
    parser.add_argument("--output-md-path", help="Output preflight Markdown path.")
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return a non-zero exit code when the preflight decision is WARNING.",
    )
    args = parser.parse_args()

    payload = write_shadow_promotion_apply_preflight_report(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        proposal_file=Path(args.proposal_file) if args.proposal_file else None,
        approval_file=Path(args.approval_file) if args.approval_file else None,
        production_profile_path=Path(args.production_profile),
        current_shadow_weights_path=(
            Path(args.current_shadow_weights_path) if args.current_shadow_weights_path else None
        ),
        target_profile_name=args.target_profile_name,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["outputs"]
    diff = payload["diff_preview"]
    print(f"Shadow promotion apply preflight：{payload['preflight_decision']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"promotion_executed：{payload['promotion_executed']}")
    print(f"apply_executed：{payload['apply_executed']}")
    print(f"preflight_only：{payload['preflight_only']}")
    print(f"safe_for_production：{payload['safe_for_production']}")
    print(f"changed_weight_keys：{', '.join(diff.get('changed_weight_keys', [])) or 'none'}")
    print(f"Preflight JSON：{outputs['json']}")
    print(f"Preflight Markdown：{outputs['markdown']}")
    if args.fail_on_warning and payload["preflight_decision"] == "WARNING":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
