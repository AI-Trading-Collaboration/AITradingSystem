from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.brokers.ibkr_paper_order import (  # noqa: E402
    DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH,
)
from ai_trading_system.trading_engine.brokers.paperbroker_ibkr_comparison import (  # noqa: E402
    DEFAULT_COMPARISON_OUTPUT_DIR,
    run_paperbroker_vs_ibkr_paper_comparison,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare local PaperBroker with IBKR Paper lifecycle behavior."
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Comparison report date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--config-path",
        "--config",
        dest="config_path",
        default=str(DEFAULT_IBKR_PAPER_ORDER_CONFIG_PATH),
        help="Path to IBKR Paper order lifecycle YAML config.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_COMPARISON_OUTPUT_DIR),
        help="Directory for comparison JSON and Markdown outputs.",
    )
    parser.add_argument(
        "--intent-fixture",
        default=None,
        help="Optional JSON fixture containing an explicit OrderIntent.",
    )
    parser.add_argument("--symbol", default="NVDA", help="Whitelisted stock symbol.")
    parser.add_argument("--side", default="BUY", help="First version only allows BUY.")
    parser.add_argument("--quantity", type=int, default=1, help="First version only allows 1.")
    parser.add_argument(
        "--limit-price",
        type=float,
        default=None,
        help="Far-from-market LIMIT price for diagnostic lifecycle comparison.",
    )
    args = parser.parse_args()
    payload = run_paperbroker_vs_ibkr_paper_comparison(
        as_of=date.fromisoformat(args.date),
        config_path=Path(args.config_path),
        output_dir=Path(args.output_dir),
        intent_fixture_path=Path(args.intent_fixture) if args.intent_fixture else None,
        symbol=args.symbol,
        side=args.side,
        quantity=args.quantity,
        limit_price=args.limit_price,
    )
    print(f"Comparison status：{payload['comparison_status']}")
    print(f"comparison_mode={payload['comparison_mode']}")
    print(f"production_effect={payload['production_effect']}")
    print(f"JSON：{payload['output_paths']['json']}")
    print(f"Markdown：{payload['output_paths']['markdown']}")


if __name__ == "__main__":
    main()
