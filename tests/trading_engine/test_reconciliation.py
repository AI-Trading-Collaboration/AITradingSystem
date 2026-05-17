from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.trading_engine.portfolio import PaperPortfolio
from ai_trading_system.trading_engine.portfolio.reconciliation import (
    load_execution_reports_from_fill_log,
    reconcile_portfolio_from_execution_reports,
)
from ai_trading_system.trading_engine.schemas import (
    BrokerOrder,
    ExecutionReport,
    OrderSide,
    OrderStatus,
    OrderType,
    RiskSeverity,
)


def test_reconciliation_passes_normal_fill() -> None:
    order = _order(quantity=5)
    execution_report = _execution_report(
        broker_order_id=order.broker_order_id,
        status=OrderStatus.FILLED,
        filled_quantity=5,
        avg_fill_price=100.0,
    )
    portfolio = PaperPortfolio(100000.0)
    portfolio.apply_fill(
        symbol="TSM",
        side=OrderSide.BUY,
        quantity=5,
        price=100.0,
        fees=0.0,
    )

    result = reconcile_portfolio_from_execution_reports(
        execution_reports=[execution_report],
        submitted_orders=[order],
        actual_portfolio=portfolio.snapshot(prices={"TSM": 101.0}, as_of=_as_of()),
        initial_cash_usd=100000.0,
        prices={"TSM": 101.0},
        as_of=_as_of(),
    )

    assert result.status == RiskSeverity.PASS
    assert result.issues == []
    assert result.production_effect == "none"


def test_reconciliation_blocks_cash_quantity_and_avg_cost_mismatch() -> None:
    order = _order(quantity=5)
    execution_report = _execution_report(
        broker_order_id=order.broker_order_id,
        status=OrderStatus.FILLED,
        filled_quantity=5,
        avg_fill_price=100.0,
    )
    portfolio = PaperPortfolio(100000.0)
    portfolio.apply_fill(
        symbol="TSM",
        side=OrderSide.BUY,
        quantity=4,
        price=101.0,
        fees=0.0,
    )

    result = reconcile_portfolio_from_execution_reports(
        execution_reports=[execution_report],
        submitted_orders=[order],
        actual_portfolio=portfolio.snapshot(prices={"TSM": 101.0}, as_of=_as_of()),
        initial_cash_usd=100000.0,
        prices={"TSM": 101.0},
        as_of=_as_of(),
    )

    assert result.status == RiskSeverity.BLOCK
    issue_fields = {issue.field for issue in result.issues}
    assert {"cash_usd", "position_quantity", "avg_cost"}.issubset(issue_fields)
    assert all(issue.severity == RiskSeverity.BLOCK for issue in result.issues)


def test_reconciliation_passes_rejected_order_without_fill() -> None:
    portfolio = PaperPortfolio(100000.0)
    rejected = ExecutionReport.rejected(
        intent_id="intent_rejected",
        requested_quantity=0,
        rejection_reason="min_confidence",
    )

    result = reconcile_portfolio_from_execution_reports(
        execution_reports=[rejected],
        submitted_orders=[],
        actual_portfolio=portfolio.snapshot(as_of=_as_of()),
        initial_cash_usd=100000.0,
        as_of=_as_of(),
    )

    assert result.status == RiskSeverity.PASS
    assert result.actual_portfolio.positions == []


def test_reconciliation_passes_cancelled_order_without_fill() -> None:
    order = _order(quantity=5, status=OrderStatus.CANCELLED)
    cancelled = _execution_report(
        broker_order_id=order.broker_order_id,
        status=OrderStatus.CANCELLED,
        filled_quantity=0,
        avg_fill_price=None,
    )
    portfolio = PaperPortfolio(100000.0)

    result = reconcile_portfolio_from_execution_reports(
        execution_reports=[cancelled],
        submitted_orders=[order],
        actual_portfolio=portfolio.snapshot(as_of=_as_of()),
        initial_cash_usd=100000.0,
        as_of=_as_of(),
    )

    assert result.status == RiskSeverity.PASS
    assert result.expected_portfolio.cash_usd == 100000.0


def test_reconciliation_passes_partially_filled_open_order() -> None:
    order = _order(quantity=10, status=OrderStatus.PARTIALLY_FILLED)
    partial = _execution_report(
        broker_order_id=order.broker_order_id,
        status=OrderStatus.PARTIALLY_FILLED,
        requested_quantity=10,
        filled_quantity=4,
        avg_fill_price=100.0,
    )
    portfolio = PaperPortfolio(100000.0)
    portfolio.apply_fill(
        symbol="TSM",
        side=OrderSide.BUY,
        quantity=4,
        price=100.0,
        fees=0.0,
    )

    result = reconcile_portfolio_from_execution_reports(
        execution_reports=[partial],
        submitted_orders=[order],
        actual_portfolio=portfolio.snapshot(prices={"TSM": 102.0}, as_of=_as_of()),
        initial_cash_usd=100000.0,
        prices={"TSM": 102.0},
        as_of=_as_of(),
    )

    assert result.status == RiskSeverity.PASS
    assert result.actual_portfolio.position_for("TSM").quantity == 4


def test_reconciliation_can_read_execution_reports_from_fill_log(tmp_path: Path) -> None:
    path = tmp_path / "fill_log" / "2026-05-17.jsonl"
    path.parent.mkdir(parents=True)
    report = _execution_report(
        broker_order_id="paper_order_000001",
        status=OrderStatus.FILLED,
        filled_quantity=5,
        avg_fill_price=100.0,
    )
    path.write_text(
        json.dumps({"payload": report.model_dump(mode="json")}) + "\n",
        encoding="utf-8",
    )

    reports = load_execution_reports_from_fill_log(
        tmp_path,
        as_of=_as_of().date(),
    )

    assert len(reports) == 1
    assert reports[0].status == OrderStatus.FILLED


def _order(
    *,
    quantity: int,
    status: OrderStatus = OrderStatus.SUBMITTED,
) -> BrokerOrder:
    return BrokerOrder(
        broker_order_id="paper_order_000001",
        client_order_id="intent_1",
        intent_id="intent_1",
        symbol="TSM",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        limit_price=100.0,
        quantity=quantity,
        status=status,
        submitted_at=_as_of(),
    )


def _execution_report(
    *,
    broker_order_id: str,
    status: OrderStatus,
    filled_quantity: int,
    avg_fill_price: float | None,
    requested_quantity: int = 5,
) -> ExecutionReport:
    return ExecutionReport(
        intent_id="intent_1",
        broker_order_id=broker_order_id,
        status=status,
        submitted_at=_as_of(),
        completed_at=_as_of() if status != OrderStatus.SUBMITTED else None,
        requested_quantity=requested_quantity,
        filled_quantity=filled_quantity,
        avg_fill_price=avg_fill_price,
        fees=0.0,
    )


def _as_of() -> datetime:
    return datetime(2026, 5, 17, 20, 0, tzinfo=UTC)
