from __future__ import annotations

import argparse
import sys
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def run_demo(
    *,
    as_of: date,
    audit_root: Path,
    report_dir: Path,
) -> dict[str, Any]:
    from ai_trading_system.trading_engine.audit import JsonlAuditLogger
    from ai_trading_system.trading_engine.config import load_trading_engine_config
    from ai_trading_system.trading_engine.execution import ExecutionService, PaperBroker
    from ai_trading_system.trading_engine.intent_builder import (
        DecisionCandidate,
        build_order_intent,
    )
    from ai_trading_system.trading_engine.portfolio import PaperPortfolio
    from ai_trading_system.trading_engine.reports import (
        build_trading_daily_report,
        write_trading_daily_report,
    )
    from ai_trading_system.trading_engine.risk import PreTradeRiskChecker
    from ai_trading_system.trading_engine.schemas import (
        AssetType,
        MarketContext,
        MarketSnapshot,
        OrderSide,
    )

    config = load_trading_engine_config()
    portfolio = PaperPortfolio(config.execution.default_initial_cash_usd)
    broker = PaperBroker(portfolio=portfolio, execution_settings=config.execution)
    service = ExecutionService(
        risk_checker=PreTradeRiskChecker(config),
        broker=broker,
        audit_logger=JsonlAuditLogger(audit_root),
        config=config,
    )
    run_id = f"paper_demo_{as_of.isoformat()}"
    created_at = datetime.combine(as_of, time(14, 0), tzinfo=UTC)
    candidates = [
        DecisionCandidate(
            created_at=created_at,
            strategy_id="paper_demo_trend_signal",
            strategy_version="mvp_2026_05_17",
            run_id=run_id,
            symbol="TSM",
            asset_type=AssetType.STOCK,
            side=OrderSide.BUY,
            target_notional_usd=4000.0,
            limit_price=185.0,
            confidence=0.73,
            score_snapshot_id="demo_score_snapshot_tsm",
            reason_codes=["sector_momentum_positive", "risk_score_acceptable"],
            metadata={"mode": "shadow", "source": "simulated_trend_signal"},
        ),
        DecisionCandidate(
            created_at=created_at,
            strategy_id="paper_demo_trend_signal",
            strategy_version="mvp_2026_05_17",
            run_id=run_id,
            symbol="NVDA",
            asset_type=AssetType.STOCK,
            side=OrderSide.BUY,
            target_notional_usd=3000.0,
            limit_price=900.0,
            confidence=0.70,
            score_snapshot_id="demo_score_snapshot_nvda",
            reason_codes=["ai_cycle_leader", "trend_confirmed"],
            metadata={"mode": "shadow", "source": "simulated_trend_signal"},
        ),
        DecisionCandidate(
            created_at=created_at,
            strategy_id="paper_demo_trend_signal",
            strategy_version="mvp_2026_05_17",
            run_id=run_id,
            symbol="INTC",
            asset_type=AssetType.STOCK,
            side=OrderSide.BUY,
            target_notional_usd=1000.0,
            limit_price=30.0,
            confidence=0.50,
            score_snapshot_id="demo_score_snapshot_intc",
            reason_codes=["low_confidence_demo_reject"],
            metadata={"mode": "shadow", "source": "simulated_trend_signal"},
        ),
    ]
    intents = [build_order_intent(candidate) for candidate in candidates]
    market_context = MarketContext(
        as_of=as_of,
        prices={"TSM": 185.0, "NVDA": 915.0, "INTC": 30.0},
    )
    for intent in intents:
        service.execute(intent, market_context=market_context)

    snapshot_time = datetime.combine(as_of, time(15, 55), tzinfo=UTC)
    fill_reports = service.process_market_snapshot(
        [
            MarketSnapshot(
                symbol="TSM",
                timestamp=snapshot_time,
                open=184.8,
                high=186.0,
                low=184.0,
                last=186.0,
            ),
            MarketSnapshot(
                symbol="NVDA",
                timestamp=snapshot_time,
                open=915.0,
                high=920.0,
                low=910.0,
                last=915.0,
            ),
        ]
    )
    final_orders = [
        broker.get_order(order.broker_order_id) for order in service.submitted_orders
    ]
    portfolio_state = service.get_portfolio_state(
        prices={"TSM": 186.0, "NVDA": 915.0, "INTC": 30.0},
        as_of=snapshot_time,
    )
    report = build_trading_daily_report(
        as_of=as_of,
        order_intents=intents,
        risk_results=service.risk_results,
        submitted_orders=final_orders,
        execution_reports=service.execution_reports,
        portfolio_state=portfolio_state,
        audit_root=audit_root,
    )
    report_path = write_trading_daily_report(
        report,
        report_dir / f"{as_of.isoformat()}.md",
    )
    return {
        "generated_intents": len(intents),
        "approved": report.approved_count,
        "rejected": report.rejected_count,
        "submitted": len(final_orders),
        "filled": len(fill_reports),
        "open": report.open_count,
        "report_path": report_path,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the paper trading engine demo.")
    parser.add_argument("--date", required=True, help="Trading date in YYYY-MM-DD format.")
    parser.add_argument(
        "--audit-root",
        default=str(REPO_ROOT / "data" / "trading_engine" / "audit"),
        help="Audit JSONL root directory.",
    )
    parser.add_argument(
        "--report-dir",
        default=str(REPO_ROOT / "reports" / "trading_daily"),
        help="Trading daily report output directory.",
    )
    args = parser.parse_args()
    as_of = date.fromisoformat(args.date)
    summary = run_demo(
        as_of=as_of,
        audit_root=Path(args.audit_root),
        report_dir=Path(args.report_dir),
    )
    print(f"生成 OrderIntent：{summary['generated_intents']}")
    print(f"风控通过 / 拒绝：{summary['approved']} / {summary['rejected']}")
    print(f"提交 paper 订单：{summary['submitted']}")
    print(f"成交 / 未成交：{summary['filled']} / {summary['open']}")
    print(f"交易日报：{summary['report_path']}")


if __name__ == "__main__":
    main()
