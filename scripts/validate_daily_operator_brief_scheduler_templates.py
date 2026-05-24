from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.daily_operator_brief_scheduler_template_validation import (  # noqa: E402
    should_fail_cli,
    write_daily_operator_brief_scheduler_template_validation,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate review-only daily operator brief scheduler templates."
    )
    parser.add_argument("--date", required=True, help="Validation date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default="data",
        help="Data root that contains derived/operator_briefs artifacts.",
    )
    parser.add_argument(
        "--template-metadata-file",
        default=None,
        help="TRADING-028 scheduler template metadata JSON to validate.",
    )
    parser.add_argument(
        "--templates-root",
        default=None,
        help=(
            "Safe scheduler template root. Defaults to "
            "data/derived/operator_briefs/scheduler_templates."
        ),
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Exit non-zero when validation_status is PASS_WITH_WARNINGS.",
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Exit non-zero when validation has critical findings or invalid inputs.",
    )
    args = parser.parse_args()

    payload = write_daily_operator_brief_scheduler_template_validation(
        as_of=date.fromisoformat(args.date),
        repo_root=REPO_ROOT,
        data_root=args.data_root,
        template_metadata_file=args.template_metadata_file,
        templates_root=args.templates_root,
    )
    coverage = payload["coverage"]
    alerts = payload["alerts"]
    outputs = payload["output_artifacts"]
    print(f"Scheduler Template Validation：{payload['validation_status']}")
    print(f"summary_level：{payload['summary_level']}")
    print(f"headline：{payload['headline']}")
    print(f"templates_declared：{coverage['templates_declared']}")
    print(f"templates_found：{coverage['templates_found']}")
    print(f"templates_passed：{coverage['templates_passed']}")
    print(f"templates_with_warnings：{coverage['templates_with_warnings']}")
    print(f"templates_failed：{coverage['templates_failed']}")
    print(f"critical_findings：{len(alerts['critical'])}")
    print(f"warnings：{len(alerts['warnings'])}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print("scheduler_template_validation_only：" f"{payload['scheduler_template_validation_only']}")
    print(f"read_only：{payload['read_only']}")
    print(f"scheduler_created：{payload['scheduler_created']}")
    print(f"scheduler_installed：{payload['scheduler_installed']}")
    print(f"scheduler_enabled：{payload['scheduler_enabled']}")
    print(f"templates_executed_by_validator：{payload['templates_executed_by_validator']}")
    print(
        "operator_brief_executed_by_validator："
        f"{payload['operator_brief_executed_by_validator']}"
    )
    print(f"pipelines_executed_by_validator：{payload['pipelines_executed_by_validator']}")
    print(f"data_downloaded_by_validator：{payload['data_downloaded_by_validator']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"Validation JSON：{outputs['validation_json']['path']}")
    print(f"Validation Markdown：{outputs['validation_markdown']['path']}")
    if should_fail_cli(
        payload,
        fail_on_warning=args.fail_on_warning,
        fail_on_critical=args.fail_on_critical,
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
