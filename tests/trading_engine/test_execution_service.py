from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.trading_engine.audit import JsonlAuditLogger
from ai_trading_system.trading_engine.audit.jsonl import read_jsonl
from ai_trading_system.trading_engine.config import TradingEngineConfig, load_trading_engine_config
from ai_trading_system.trading_engine.execution import ExecutionService, PaperBroker
from ai_trading_system.trading_engine.portfolio import PaperPortfolio
from ai_trading_system.trading_engine.risk import PreTradeRiskChecker
from ai_trading_system.trading_engine.schemas import (
    AssetType,
    BrokerOrder,
    MarketContext,
    MarketSnapshot,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PortfolioState,
    RiskCheckResult,
    RiskRuleEvaluation,
    TimeInForce,
)


def test_execution_service_calls_risk_checker_before_submit() -> None:
    broker = SpyBroker(portfolio=PaperPortfolio(100000.0))
    checker = SpyRiskChecker(approved=True)
    service = ExecutionService(
        risk_checker=checker,
        broker=broker,
        config=load_trading_engine_config(),
    )

    report = service.execute(_intent())

    assert checker.called is True
    assert broker.submitted is True
    assert report.status == OrderStatus.SUBMITTED


def test_execution_service_does_not_submit_rejected_order() -> None:
    broker = SpyBroker(portfolio=PaperPortfolio(100000.0))
    checker = SpyRiskChecker(approved=False)
    service = ExecutionService(
        risk_checker=checker,
        broker=broker,
        config=load_trading_engine_config(),
    )

    report = service.execute(_intent())

    assert checker.called is True
    assert broker.submitted is False
    assert report.status == OrderStatus.REJECTED
    assert "test_block" in (report.rejection_reason or "")


def test_execution_service_counts_submitted_orders_for_duplicate_guard() -> None:
    config = load_trading_engine_config()
    broker = PaperBroker(portfolio=PaperPortfolio(100000.0))
    service = ExecutionService(
        risk_checker=PreTradeRiskChecker(config),
        broker=broker,
        config=config,
    )
    context = MarketContext(as_of=date(2026, 5, 17))

    first = service.execute(_intent(intent_id="intent_1"), market_context=context)
    second = service.execute(_intent(intent_id="intent_2"), market_context=context)
    third = service.execute(_intent(intent_id="intent_3"), market_context=context)

    assert first.status == OrderStatus.SUBMITTED
    assert second.status == OrderStatus.SUBMITTED
    assert third.status == OrderStatus.REJECTED
    assert "max_daily_trades_per_symbol" in (third.rejection_reason or "")


def test_execution_service_writes_audit_logs(tmp_path: Path) -> None:
    audit_root = tmp_path / "audit"
    broker = PaperBroker(portfolio=PaperPortfolio(100000.0))
    service = ExecutionService(
        risk_checker=PreTradeRiskChecker(load_trading_engine_config()),
        broker=broker,
        audit_logger=JsonlAuditLogger(audit_root),
        config=load_trading_engine_config(),
    )
    intent = _intent(target_quantity=5, target_notional_usd=None)

    service.execute(intent, market_context=MarketContext(as_of=date(2026, 5, 17)))
    service.process_market_snapshot(
        MarketSnapshot(
            symbol="TSM",
            timestamp=datetime(2026, 5, 17, 20, 0, tzinfo=UTC),
            open=99.5,
            high=101.0,
            low=99.0,
            last=101.0,
        )
    )

    order_intent_log = audit_root / "order_intent_log" / "2026-05-17.jsonl"
    risk_log = audit_root / "risk_check_log" / "2026-05-17.jsonl"
    order_log = audit_root / "order_log" / "2026-05-17.jsonl"
    fill_log = audit_root / "fill_log" / "2026-05-17.jsonl"
    portfolio_log = audit_root / "portfolio_snapshot" / "2026-05-17.jsonl"
    assert len(read_jsonl(order_intent_log)) == 1
    assert len(read_jsonl(risk_log)) == 1
    assert len(read_jsonl(order_log)) == 1
    assert len(read_jsonl(fill_log)) == 1
    assert len(read_jsonl(portfolio_log)) == 1


def test_execution_service_rejects_real_mode_config() -> None:
    config = _config_with_mode(mode="real")

    with pytest.raises(RuntimeError, match="Real trading mode"):
        ExecutionService(config=config)


class SpyRiskChecker:
    def __init__(self, *, approved: bool) -> None:
        self.approved = approved
        self.called = False

    def check(
        self,
        order_intent: OrderIntent,
        portfolio_state: PortfolioState,
        market_context: MarketContext | None = None,
    ) -> RiskCheckResult:
        self.called = True
        evaluation = RiskRuleEvaluation(
            rule_id="test_pass" if self.approved else "test_block",
            passed=self.approved,
        )
        return RiskCheckResult.from_evaluations(
            intent_id=order_intent.intent_id,
            evaluations=[evaluation],
            risk_config_version="risk_test",
        )


class SpyBroker(PaperBroker):
    def __init__(self, *, portfolio: PaperPortfolio) -> None:
        super().__init__(portfolio=portfolio)
        self.submitted = False

    def submit_order(self, order_intent: OrderIntent) -> BrokerOrder:
        self.submitted = True
        return super().submit_order(order_intent)


def _config_with_mode(*, mode: str) -> TradingEngineConfig:
    config = load_trading_engine_config()
    trading = config.trading.model_copy(update={"mode": mode})
    return config.model_copy(update={"trading": trading})


def _intent(**overrides: object) -> OrderIntent:
    values = {
        "created_at": datetime(2026, 5, 17, 14, 0, tzinfo=UTC),
        "strategy_id": "execution_test_strategy",
        "strategy_version": "v1",
        "run_id": "run_2026_05_17",
        "symbol": "TSM",
        "asset_type": AssetType.STOCK,
        "side": OrderSide.BUY,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "target_notional_usd": 1000.0,
        "limit_price": 100.0,
        "confidence": 0.75,
        "score_snapshot_id": "score_snapshot_1",
    }
    values.update(overrides)
    return OrderIntent.model_validate(values)
