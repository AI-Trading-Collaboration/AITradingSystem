from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.shadow_promotion_lifecycle_audit import (  # noqa: E402
    write_shadow_promotion_lifecycle_audit_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a read-only shadow promotion lifecycle audit report."
    )
    parser.add_argument("--date", required=True, help="Audit run date in YYYY-MM-DD format.")
    parser.add_argument(
        "--promotion-date",
        help="Promotion event date in YYYY-MM-DD format. Defaults to --date.",
    )
    parser.add_argument(
        "--data-root",
        default=str(REPO_ROOT / "data"),
        help="Data root containing existing promotion artifacts.",
    )
    parser.add_argument("--proposal-file", help="TRADING-018D promotion proposal JSON path.")
    parser.add_argument("--preflight-file", help="TRADING-018E1 apply preflight JSON path.")
    parser.add_argument("--apply-result-file", help="TRADING-018E2 apply result JSON path.")
    parser.add_argument(
        "--rollback-result-file",
        help="Optional TRADING-018E3 rollback result JSON path.",
    )
    parser.add_argument(
        "--include-approval-artifacts",
        action="store_true",
        help="Include optional approval artifact records when present.",
    )
    parser.add_argument(
        "--fail-on-safety-anomaly",
        action="store_true",
        help="Write the audit artifacts, then exit non-zero if SAFETY_ANOMALY is detected.",
    )
    parser.add_argument("--output-json-path", help="Output lifecycle audit JSON path.")
    parser.add_argument("--output-md-path", help="Output lifecycle audit Markdown path.")
    args = parser.parse_args()

    as_of = date.fromisoformat(args.date)
    promotion_date = date.fromisoformat(args.promotion_date) if args.promotion_date else as_of
    payload = write_shadow_promotion_lifecycle_audit_report(
        as_of=as_of,
        promotion_date=promotion_date,
        data_root=Path(args.data_root),
        proposal_file=Path(args.proposal_file) if args.proposal_file else None,
        preflight_file=Path(args.preflight_file) if args.preflight_file else None,
        apply_result_file=Path(args.apply_result_file) if args.apply_result_file else None,
        rollback_result_file=Path(args.rollback_result_file) if args.rollback_result_file else None,
        include_approval_artifacts=args.include_approval_artifacts,
        fail_on_safety_anomaly=args.fail_on_safety_anomaly,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["outputs"]
    findings = payload["audit_findings"]
    print(f"Shadow promotion lifecycle audit：{payload['lifecycle_decision']}")
    print(f"promotion_date：{payload['promotion_date']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"audit_only：{payload['audit_only']}")
    print(f"apply_executed_by_audit：{payload['apply_executed_by_audit']}")
    print(f"rollback_executed_by_audit：{payload['rollback_executed_by_audit']}")
    print(f"safe_for_scheduler：{payload['safe_for_scheduler']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"critical_findings：{len(findings.get('critical_findings', []))}")
    print(f"warnings：{len(findings.get('warnings', []))}")
    print(f"Audit JSON：{outputs['json']}")
    print(f"Audit Markdown：{outputs['markdown']}")


if __name__ == "__main__":
    main()
