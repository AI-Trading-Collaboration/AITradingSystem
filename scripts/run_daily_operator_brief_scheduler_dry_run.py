from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.daily_operator_brief_scheduler_dry_run import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    DEFAULT_EXPECTED_RUN_HOUR,
    DEFAULT_EXPECTED_RUN_MINUTE,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_TIMEZONE,
    write_daily_operator_brief_scheduler_dry_run,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the read-only daily operator brief scheduler dry run."
    )
    parser.add_argument("--date", required=True, help="Dry-run date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Data root containing existing operator brief and upstream artifacts.",
    )
    parser.add_argument(
        "--expected-run-hour",
        type=int,
        default=DEFAULT_EXPECTED_RUN_HOUR,
        help="Expected local scheduler run hour.",
    )
    parser.add_argument(
        "--expected-run-minute",
        type=int,
        default=DEFAULT_EXPECTED_RUN_MINUTE,
        help="Expected local scheduler run minute.",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help="Timezone label for the intended future schedule.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Calendar-day lookback window for latest input artifacts.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat optional health/freshness dependency gaps as not ready.",
    )
    parser.add_argument(
        "--fail-on-missing-required",
        action="store_true",
        help="Write artifacts, then exit non-zero if required inputs are missing.",
    )
    parser.add_argument("--output-json-path", help="Output scheduler dry-run JSON path.")
    parser.add_argument("--output-md-path", help="Output scheduler dry-run Markdown path.")
    args = parser.parse_args()

    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        expected_run_hour=args.expected_run_hour,
        expected_run_minute=args.expected_run_minute,
        timezone=args.timezone,
        lookback_days=args.lookback_days,
        strict=args.strict,
        fail_on_missing_required=args.fail_on_missing_required,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = payload["output_artifacts"]
    dependency = payload["dependency_check"]
    safety = payload["safety_check"]
    behavior = payload["expected_operator_brief_behavior"]

    print(f"Daily Operator Brief Scheduler Dry Run：{payload['dry_run_decision']}")
    print(f"dry_run_status：{payload['dry_run_status']}")
    print(f"summary_level：{payload['summary_level']}")
    print(f"safe_for_scheduled_generation：{payload['safe_for_scheduled_generation']}")
    print(f"dependency_check.status：{dependency['status']}")
    print(f"safety_check.status：{safety['status']}")
    print(f"missing_required_inputs：{len(dependency['missing_required_inputs'])}")
    print(f"missing_optional_inputs：{len(dependency['missing_optional_inputs'])}")
    print(f"stale_inputs：{len(dependency['stale_inputs'])}")
    print(f"expected_degradation：{behavior['expected_degradation']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"scheduler_dry_run_only：{payload['scheduler_dry_run_only']}")
    print(f"read_only：{payload['read_only']}")
    print(f"scheduler_created：{payload['scheduler_created']}")
    print(
        "operator_brief_executed_by_scheduler_dry_run："
        f"{payload['operator_brief_executed_by_scheduler_dry_run']}"
    )
    print(
        "pipelines_executed_by_scheduler_dry_run："
        f"{payload['pipelines_executed_by_scheduler_dry_run']}"
    )
    print(
        "data_downloaded_by_scheduler_dry_run："
        f"{payload['data_downloaded_by_scheduler_dry_run']}"
    )
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"Scheduler Dry Run JSON：{outputs['json']['path']}")
    print(f"Scheduler Dry Run Markdown：{outputs['markdown']['path']}")


if __name__ == "__main__":
    main()
