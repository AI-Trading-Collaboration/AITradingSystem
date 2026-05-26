from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from ai_trading_system.fundamentals.sec_pit_evaluation import (
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SEC_PIT_EVALUATION_CONFIG_PATH,
    DEFAULT_SEC_PIT_EVALUATION_OUTPUT_DIR,
    DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
    run_sec_pit_evaluation,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run TRADING-040 SEC PIT evaluation.")
    parser.add_argument("--start", required=True, type=_parse_date)
    parser.add_argument("--end", required=True, type=_parse_date)
    parser.add_argument("--feature-panel", type=Path, default=DEFAULT_SEC_PIT_FEATURE_PANEL_PATH)
    parser.add_argument("--universe", type=Path, default=Path("config/sec_companies.yaml"))
    parser.add_argument("--benchmark", default="QQQ")
    parser.add_argument("--tickers", nargs="*", default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_SEC_PIT_EVALUATION_OUTPUT_DIR)
    parser.add_argument("--prices-path", type=Path, default=DEFAULT_PRICES_PATH)
    parser.add_argument("--rates-path", type=Path, default=DEFAULT_RATES_PATH)
    parser.add_argument("--policy-path", type=Path, default=DEFAULT_SEC_PIT_EVALUATION_CONFIG_PATH)
    args = parser.parse_args()
    artifacts = run_sec_pit_evaluation(
        start=args.start,
        end=args.end,
        feature_panel_path=args.feature_panel,
        universe_path=args.universe,
        benchmark=args.benchmark,
        tickers=args.tickers,
        output_dir=args.output_dir,
        prices_path=args.prices_path,
        rates_path=args.rates_path,
        policy_path=args.policy_path,
    )
    print(f"status={artifacts.status}")
    print(f"summary_json={artifacts.summary_json_path}")
    print(f"summary_markdown={artifacts.summary_markdown_path}")
    print(f"feature_effectiveness={artifacts.feature_effectiveness_path}")
    print(f"signal_attribution={artifacts.signal_attribution_path}")
    print(f"shadow_candidate_weights={artifacts.shadow_candidate_weights_path}")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


if __name__ == "__main__":
    main()
