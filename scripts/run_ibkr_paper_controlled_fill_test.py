from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.brokers.ibkr_paper_controlled_fill import (  # noqa: E402
    DEFAULT_IBKR_PAPER_CONTROLLED_FILL_CONFIG_PATH,
    run_ibkr_paper_controlled_fill_test,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an IBKR Paper controlled small fill test.")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Controlled fill report date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--config-path",
        "--config",
        dest="config_path",
        default=str(DEFAULT_IBKR_PAPER_CONTROLLED_FILL_CONFIG_PATH),
        help="Path to IBKR Paper controlled fill YAML config.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory for controlled fill JSON and Markdown outputs.",
    )
    parser.add_argument("--symbol", default="NVDA", help="Whitelisted stock symbol.")
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"], help="Order side.")
    parser.add_argument("--quantity", type=int, default=1, help="Controlled paper quantity.")
    parser.add_argument(
        "--limit-price",
        type=float,
        required=True,
        help="Manual LIMIT price confirmed by the operator before running.",
    )
    parser.add_argument(
        "--allow-outside-rth-diagnostic",
        action="store_true",
        help=(
            "Allow a diagnostic controlled fill submission outside regular trading hours. "
            "The report remains LIMITED and marks outside_rth_override=true."
        ),
    )
    args = parser.parse_args()

    payload = run_ibkr_paper_controlled_fill_test(
        as_of=date.fromisoformat(args.date),
        config_path=Path(args.config_path),
        output_dir=Path(args.output_dir),
        symbol=args.symbol,
        side=args.side,
        quantity=args.quantity,
        limit_price=args.limit_price,
        allow_outside_rth_diagnostic=args.allow_outside_rth_diagnostic,
    )
    print(f"Controlled fill status：{payload['test_status']}")
    print(f"Account：{payload['account_id_masked']}")
    print(f"Market session：{payload['market_session_status']}")
    print(f"Controlled fill submission：{payload['controlled_fill_submission']}")
    print(f"Outside RTH override：{payload['outside_rth_override']}")
    print(f"Fill seen：{payload['fill_seen']}")
    print(f"Avg fill price：{payload['avg_fill_price']}")
    print(f"Cancel requested：{payload['cancel_requested']}")
    print(f"Final order status：{payload['final_order_status']}")
    print(f"JSON：{payload['output_paths']['json']}")
    print(f"Markdown：{payload['output_paths']['markdown']}")


if __name__ == "__main__":
    main()
