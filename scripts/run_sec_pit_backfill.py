from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.fundamentals.sec_pit_backfill import (  # noqa: E402
    DEFAULT_SEC_EDGAR_PROCESSED_DIR,
    DEFAULT_SEC_EDGAR_RAW_DIR,
    DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH,
    DEFAULT_SEC_PIT_REPORT_DIR,
    run_sec_pit_backfill,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run SEC EDGAR reconstructed filing-time PIT backfill."
    )
    parser.add_argument("--from", dest="start", required=True, help="Start date YYYY-MM-DD.")
    parser.add_argument("--to", dest="end", required=True, help="End date YYYY-MM-DD.")
    parser.add_argument(
        "--ticker",
        action="append",
        default=None,
        help="Ticker to process; may be repeated. Defaults to active SEC companies.",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help="SEC fair access User-Agent. Defaults to SEC_USER_AGENT.",
    )
    parser.add_argument("--raw-dir", default=str(DEFAULT_SEC_EDGAR_RAW_DIR))
    parser.add_argument("--processed-dir", default=str(DEFAULT_SEC_EDGAR_PROCESSED_DIR))
    parser.add_argument("--report-dir", default=str(DEFAULT_SEC_PIT_REPORT_DIR))
    parser.add_argument("--config", default=str(DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH))
    parser.add_argument(
        "--build-from-existing-raw",
        action="store_true",
        help="Skip live raw fetch and rebuild downstream artifacts from existing raw files.",
    )
    parser.add_argument("--no-cache", action="store_true", help="Bypass external request cache.")
    args = parser.parse_args()

    user_agent = args.user_agent or os.getenv("SEC_USER_AGENT")
    artifacts = run_sec_pit_backfill(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        raw_dir=Path(args.raw_dir),
        processed_dir=Path(args.processed_dir),
        report_dir=Path(args.report_dir),
        config_path=Path(args.config),
        user_agent=user_agent,
        tickers=args.ticker,
        use_cache=not args.no_cache,
        full_pipeline=not args.build_from_existing_raw,
    )
    for name, path in artifacts.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
