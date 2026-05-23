from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.pipeline_health_summary import (  # noqa: E402
    DEFAULT_DATA_ROOT,
    DEFAULT_FRESHNESS_DAYS,
    DEFAULT_LOOKBACK_DAYS,
    write_pipeline_health_summary,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a read-only pipeline health summary from existing artifacts."
    )
    parser.add_argument("--date", required=True, help="Summary date in YYYY-MM-DD format.")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Data root containing existing derived pipeline artifacts.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Calendar-day lookback window for latest artifacts.",
    )
    parser.add_argument(
        "--freshness-days",
        type=int,
        default=DEFAULT_FRESHNESS_DAYS,
        help="Default freshness threshold for pipelines without registry overrides.",
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Write artifacts, then exit non-zero if health_status is CRITICAL.",
    )
    parser.add_argument(
        "--include-optional-pipelines",
        action="store_true",
        help="Also scan optional non-daily promotion and support pipelines.",
    )
    parser.add_argument("--output-json-path", help="Output pipeline health JSON path.")
    parser.add_argument("--output-md-path", help="Output pipeline health Markdown path.")
    args = parser.parse_args()

    payload = write_pipeline_health_summary(
        as_of=date.fromisoformat(args.date),
        data_root=Path(args.data_root),
        lookback_days=args.lookback_days,
        freshness_days=args.freshness_days,
        fail_on_critical=args.fail_on_critical,
        include_optional_pipelines=args.include_optional_pipelines,
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    coverage = payload["coverage"]
    outputs = payload["output_artifacts"]
    alerts = payload["alerts"]

    print(f"Pipeline Health Summary：{payload['health_status']}")
    print(f"summary_level：{payload['summary_level']}")
    print(f"headline：{payload['headline']}")
    print(f"registered_pipelines：{coverage['registered_pipelines']}")
    print(f"required_pipelines：{coverage['required_pipelines']}")
    print(f"available_pipelines：{coverage['available_pipelines']}")
    print(f"missing_required_pipelines：{coverage['missing_required_pipelines']}")
    print(f"stale_required_pipelines：{coverage['stale_required_pipelines']}")
    print(f"critical_pipelines：{coverage['critical_pipelines']}")
    print(f"warning_pipelines：{coverage['warning_pipelines']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{payload['manual_review_only']}")
    print(f"pipeline_health_only：{payload['pipeline_health_only']}")
    print(f"read_only：{payload['read_only']}")
    print("pipelines_executed_by_health_check：" f"{payload['pipelines_executed_by_health_check']}")
    print(f"apply_executed_by_health_check：{payload['apply_executed_by_health_check']}")
    print(f"rollback_executed_by_health_check：{payload['rollback_executed_by_health_check']}")
    print(f"safe_for_scheduler：{payload['safe_for_scheduler']}")
    print(f"broker_execution：{payload['broker_execution']}")
    print(f"replay_execution：{payload['replay_execution']}")
    print(f"trading_execution：{payload['trading_execution']}")
    print(f"critical_alerts：{len(alerts.get('critical', []))}")
    print(f"warnings：{len(alerts.get('warnings', []))}")
    print(f"Pipeline Health JSON：{outputs['json']['path']}")
    print(f"Pipeline Health Markdown：{outputs['markdown']['path']}")


if __name__ == "__main__":
    main()
