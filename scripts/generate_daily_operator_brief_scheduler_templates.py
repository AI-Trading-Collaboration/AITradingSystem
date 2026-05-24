from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.daily_operator_brief_scheduler_templates import (  # noqa: E402
    DEFAULT_DRY_RUN_SCRIPT,
    DEFAULT_EXPECTED_RUN_HOUR,
    DEFAULT_EXPECTED_RUN_MINUTE,
    DEFAULT_OPERATOR_BRIEF_SCRIPT,
    DEFAULT_TIMEZONE,
    default_scheduler_template_root,
    write_daily_operator_brief_scheduler_templates,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate review-only scheduler configuration templates."
    )
    parser.add_argument("--date", required=True, help="Template date in YYYY-MM-DD format.")
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root to embed in generated review templates.",
    )
    parser.add_argument(
        "--python-path",
        default=sys.executable,
        help="Python executable path to embed in generated review templates.",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Safe output root under data/derived/operator_briefs/scheduler_templates.",
    )
    parser.add_argument(
        "--expected-run-hour",
        type=int,
        default=DEFAULT_EXPECTED_RUN_HOUR,
        help="Expected local scheduler run hour for template content.",
    )
    parser.add_argument(
        "--expected-run-minute",
        type=int,
        default=DEFAULT_EXPECTED_RUN_MINUTE,
        help="Expected local scheduler run minute for template content.",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help="Timezone label for the intended future schedule.",
    )
    parser.add_argument(
        "--include-windows-task-xml",
        action="store_true",
        help="When any include flag is set, include Windows Task Scheduler XML template.",
    )
    parser.add_argument(
        "--include-powershell-wrapper",
        action="store_true",
        help="When any include flag is set, include PowerShell wrapper template.",
    )
    parser.add_argument(
        "--include-cron",
        action="store_true",
        help="When any include flag is set, include cron line template.",
    )
    parser.add_argument(
        "--include-github-actions",
        action="store_true",
        help="When any include flag is set, include GitHub Actions workflow template.",
    )
    parser.add_argument(
        "--operator-brief-script",
        default=DEFAULT_OPERATOR_BRIEF_SCRIPT,
        help="Operator brief script path to include in templates.",
    )
    parser.add_argument(
        "--dry-run-script",
        default=DEFAULT_DRY_RUN_SCRIPT,
        help="Scheduler dry-run script path to include in templates.",
    )
    args = parser.parse_args()

    include_flags = (
        args.include_windows_task_xml,
        args.include_powershell_wrapper,
        args.include_cron,
        args.include_github_actions,
    )
    include_all = not any(include_flags)
    repo_root = Path(args.repo_root)
    output_root = (
        Path(args.output_root) if args.output_root else default_scheduler_template_root(repo_root)
    )

    payload = write_daily_operator_brief_scheduler_templates(
        as_of=date.fromisoformat(args.date),
        repo_root=repo_root,
        python_path=Path(args.python_path),
        output_root=output_root,
        expected_run_hour=args.expected_run_hour,
        expected_run_minute=args.expected_run_minute,
        timezone_name=args.timezone,
        include_windows_task_xml=include_all or args.include_windows_task_xml,
        include_powershell_wrapper=(
            include_all or args.include_powershell_wrapper or args.include_windows_task_xml
        ),
        include_batch_wrapper=include_all or args.include_powershell_wrapper,
        include_cron=include_all or args.include_cron,
        include_github_actions=include_all or args.include_github_actions,
        operator_brief_script=args.operator_brief_script,
        dry_run_script=args.dry_run_script,
    )
    safety = payload["safety_validation"]
    outputs = payload["output_artifacts"]
    print(f"Daily Operator Brief Scheduler Templates：{payload['template_generation_status']}")
    print(f"summary_level：{payload['summary_level']}")
    print(f"headline：{payload['headline']}")
    print(f"generated_template_count：{payload['generated_template_count']}")
    print(f"safety_validation.status：{safety['status']}")
    print(f"blocking_reasons：{len(safety['blocking_reasons'])}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"scheduler_template_only：{payload['scheduler_template_only']}")
    print(f"read_only：{payload['read_only']}")
    print(f"scheduler_created：{payload['scheduler_created']}")
    print(f"scheduler_installed：{payload['scheduler_installed']}")
    print(f"scheduler_enabled：{payload['scheduler_enabled']}")
    print(
        "operator_brief_executed_by_template_generator："
        f"{payload['operator_brief_executed_by_template_generator']}"
    )
    print(
        "pipelines_executed_by_template_generator："
        f"{payload['pipelines_executed_by_template_generator']}"
    )
    print(
        "data_downloaded_by_template_generator："
        f"{payload['data_downloaded_by_template_generator']}"
    )
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"Scheduler Template Metadata JSON：{outputs['metadata_json']['path']}")
    print(f"Scheduler Template Summary Markdown：{outputs['summary_markdown']['path']}")


if __name__ == "__main__":
    main()
