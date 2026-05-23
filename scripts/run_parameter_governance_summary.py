from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.parameter_governance_summary import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_PRODUCTION_PROFILE_PATH,
    write_parameter_governance_summary_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a read-only parameter governance summary report."
    )
    parser.add_argument("--date", required=True, help="Summary date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Data root containing existing weight iteration governance artifacts.",
    )
    parser.add_argument(
        "--production-profile",
        default=str(DEFAULT_PRODUCTION_PROFILE_PATH),
        help="Production profile JSON/YAML path to read only.",
    )
    parser.add_argument(
        "--shadow-weights-file",
        help=(
            "Current shadow weights JSON path. Defaults to "
            "data/derived/.../current_shadow_weights.json."
        ),
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Calendar-day lookback window for latest governance artifacts.",
    )
    parser.add_argument(
        "--fail-on-safety-anomaly",
        action="store_true",
        help="Write artifacts, then exit non-zero if SAFETY_ANOMALY is detected.",
    )
    parser.add_argument("--output-json-path", help="Output governance summary JSON path.")
    parser.add_argument("--output-md-path", help="Output governance summary Markdown path.")
    args = parser.parse_args()

    payload = write_parameter_governance_summary_report(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        production_profile_path=Path(args.production_profile),
        shadow_weights_file=Path(args.shadow_weights_file) if args.shadow_weights_file else None,
        lookback_days=args.lookback_days,
        fail_on_safety_anomaly=args.fail_on_safety_anomaly,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["outputs"]
    findings = payload["audit_findings"]
    print(f"Parameter governance state：{payload['governance_state']}")
    print(f"action_required：{payload['action_required']}")
    print(f"action_level：{payload['action_level']}")
    print(f"recommended_action：{payload['recommended_action']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"governance_only：{payload['governance_only']}")
    print(f"apply_executed_by_governance：{payload['apply_executed_by_governance']}")
    print(f"rollback_executed_by_governance：{payload['rollback_executed_by_governance']}")
    print(f"safe_for_scheduler：{payload['safe_for_scheduler']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"critical_findings：{len(findings.get('critical_findings', []))}")
    print(f"warnings：{len(findings.get('warnings', []))}")
    print(f"Summary JSON：{outputs['json']}")
    print(f"Summary Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
