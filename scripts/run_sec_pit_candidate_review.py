from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from ai_trading_system.fundamentals.sec_pit_baseline_comparison import (
    DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    DEFAULT_SEC_PIT_EVALUATION_DIR,
)
from ai_trading_system.fundamentals.sec_pit_candidate_review import (
    DEFAULT_SEC_PIT_CANDIDATE_REVIEW_OUTPUT_DIR,
    run_sec_pit_candidate_review,
)
from ai_trading_system.fundamentals.sec_pit_real_run_diagnostics import (
    DEFAULT_SEC_PIT_DIAGNOSTICS_OUTPUT_DIR,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run TRADING-043 SEC PIT candidate review.")
    parser.add_argument("--start", type=_parse_date, default=None)
    parser.add_argument("--end", type=_parse_date, default=None)
    parser.add_argument("--evaluation-dir", type=Path, default=DEFAULT_SEC_PIT_EVALUATION_DIR)
    parser.add_argument(
        "--comparison-dir",
        type=Path,
        default=DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    )
    parser.add_argument(
        "--diagnostics-dir",
        type=Path,
        default=DEFAULT_SEC_PIT_DIAGNOSTICS_OUTPUT_DIR,
    )
    parser.add_argument("--candidate-feature", action="append", default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_SEC_PIT_CANDIDATE_REVIEW_OUTPUT_DIR,
    )
    parser.add_argument("--latest", action="store_true")
    args = parser.parse_args()

    artifacts = run_sec_pit_candidate_review(
        start=args.start,
        end=args.end,
        evaluation_dir=args.evaluation_dir,
        comparison_dir=args.comparison_dir,
        diagnostics_dir=args.diagnostics_dir,
        candidate_features=_flatten_features(args.candidate_feature),
        output_dir=args.output_dir,
        latest=args.latest,
    )
    print(f"status={artifacts.status}")
    print(f"summary_json={artifacts.summary_json_path}")
    print(f"summary_markdown={artifacts.summary_markdown_path}")
    print(f"candidate_evidence={artifacts.candidate_evidence_path}")
    print(f"by_ticker={artifacts.by_ticker_path}")
    print(f"by_period={artifacts.by_period_path}")
    print(f"baseline_overlap={artifacts.baseline_overlap_path}")
    print(f"shadow_proposal={artifacts.shadow_proposal_path}")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _flatten_features(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    flattened: list[str] = []
    for value in values:
        flattened.extend(part.strip() for part in value.replace(",", " ").split())
    return [item for item in flattened if item]


if __name__ == "__main__":
    main()
