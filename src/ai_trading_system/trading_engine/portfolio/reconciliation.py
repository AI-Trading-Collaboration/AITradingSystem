from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

from ai_trading_system.trading_engine.portfolio.paper_portfolio import PaperPortfolio
from ai_trading_system.trading_engine.schemas.broker_order import BrokerOrder, OrderStatus
from ai_trading_system.trading_engine.schemas.execution_report import ExecutionReport
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState
from ai_trading_system.trading_engine.schemas.reconciliation import (
    ReconciliationIssue,
    ReconciliationResult,
)
from ai_trading_system.trading_engine.schemas.risk_result import RiskSeverity


def rebuild_expected_portfolio_from_execution_reports(
    *,
    execution_reports: Iterable[ExecutionReport],
    submitted_orders: Iterable[BrokerOrder],
    initial_cash_usd: float,
    prices: dict[str, float] | None = None,
    as_of: datetime | None = None,
) -> PortfolioState:
    portfolio = PaperPortfolio(initial_cash_usd)
    orders_by_id = {
        order.broker_order_id: order
        for order in submitted_orders
    }
    for report in execution_reports:
        if report.filled_quantity <= 0:
            continue
        if report.avg_fill_price is None:
            raise ValueError(
                f"filled execution report is missing avg_fill_price: {report.intent_id}"
            )
        if not report.broker_order_id or report.broker_order_id not in orders_by_id:
            raise ValueError(
                f"filled execution report cannot be matched to a submitted order: "
                f"{report.intent_id}"
            )
        order = orders_by_id[report.broker_order_id]
        portfolio.apply_fill(
            symbol=order.symbol,
            side=order.side,
            quantity=report.filled_quantity,
            price=report.avg_fill_price,
            fees=report.fees,
        )
    return portfolio.snapshot(prices=prices, as_of=as_of)


def load_execution_reports_from_fill_log(
    audit_root: Path | str,
    *,
    as_of: date,
) -> tuple[ExecutionReport, ...]:
    path = Path(audit_root) / "fill_log" / f"{as_of.isoformat()}.jsonl"
    if not path.exists():
        return ()
    reports: list[ExecutionReport] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            record = json.loads(line)
            payload = record.get("payload") if isinstance(record, dict) else None
            if isinstance(payload, dict):
                reports.append(ExecutionReport.model_validate(payload))
    return tuple(reports)


def reconcile_portfolio_states(
    *,
    expected_portfolio: PortfolioState,
    actual_portfolio: PortfolioState,
    initial_cash_usd: float,
    source: str,
    tolerance_cash_usd: float = 0.01,
    tolerance_avg_cost_usd: float = 0.0001,
) -> ReconciliationResult:
    issues: list[ReconciliationIssue] = []
    _append_cash_issue(
        issues,
        expected=expected_portfolio.cash_usd,
        actual=actual_portfolio.cash_usd,
        tolerance=tolerance_cash_usd,
    )
    for symbol in _portfolio_symbols(expected_portfolio, actual_portfolio):
        expected_position = expected_portfolio.position_for(symbol)
        actual_position = actual_portfolio.position_for(symbol)
        expected_quantity = 0 if expected_position is None else expected_position.quantity
        actual_quantity = 0 if actual_position is None else actual_position.quantity
        if expected_quantity != actual_quantity:
            issues.append(
                ReconciliationIssue(
                    field="position_quantity",
                    symbol=symbol,
                    expected=expected_quantity,
                    actual=actual_quantity,
                    message="expected and actual position quantity diverged",
                )
            )
        if expected_position is None or actual_position is None:
            continue
        if abs(expected_position.avg_cost - actual_position.avg_cost) > tolerance_avg_cost_usd:
            issues.append(
                ReconciliationIssue(
                    field="avg_cost",
                    symbol=symbol,
                    expected=expected_position.avg_cost,
                    actual=actual_position.avg_cost,
                    message="expected and actual average cost diverged",
                )
            )
    return ReconciliationResult(
        status=RiskSeverity.BLOCK if issues else RiskSeverity.PASS,
        source=source,
        initial_cash_usd=initial_cash_usd,
        tolerance_cash_usd=tolerance_cash_usd,
        tolerance_avg_cost_usd=tolerance_avg_cost_usd,
        expected_portfolio=expected_portfolio,
        actual_portfolio=actual_portfolio,
        issues=issues,
    )


def reconcile_portfolio_from_execution_reports(
    *,
    execution_reports: Iterable[ExecutionReport],
    submitted_orders: Iterable[BrokerOrder],
    actual_portfolio: PortfolioState,
    initial_cash_usd: float,
    prices: dict[str, float] | None = None,
    as_of: datetime | None = None,
    tolerance_cash_usd: float = 0.01,
    tolerance_avg_cost_usd: float = 0.0001,
) -> ReconciliationResult:
    expected = rebuild_expected_portfolio_from_execution_reports(
        execution_reports=execution_reports,
        submitted_orders=submitted_orders,
        initial_cash_usd=initial_cash_usd,
        prices=prices,
        as_of=as_of,
    )
    return reconcile_portfolio_states(
        expected_portfolio=expected,
        actual_portfolio=actual_portfolio,
        initial_cash_usd=initial_cash_usd,
        source="execution_reports",
        tolerance_cash_usd=tolerance_cash_usd,
        tolerance_avg_cost_usd=tolerance_avg_cost_usd,
    )


def reconcile_portfolio_from_fill_log(
    *,
    audit_root: Path | str,
    as_of: date,
    submitted_orders: Iterable[BrokerOrder],
    actual_portfolio: PortfolioState,
    initial_cash_usd: float,
    prices: dict[str, float] | None = None,
) -> ReconciliationResult:
    return reconcile_portfolio_from_execution_reports(
        execution_reports=load_execution_reports_from_fill_log(audit_root, as_of=as_of),
        submitted_orders=submitted_orders,
        actual_portfolio=actual_portfolio,
        initial_cash_usd=initial_cash_usd,
        prices=prices,
        as_of=actual_portfolio.as_of,
    )


def _append_cash_issue(
    issues: list[ReconciliationIssue],
    *,
    expected: float,
    actual: float,
    tolerance: float,
) -> None:
    if abs(expected - actual) <= tolerance:
        return
    issues.append(
        ReconciliationIssue(
            field="cash_usd",
            expected=expected,
            actual=actual,
            message="expected and actual cash diverged",
        )
    )


def _portfolio_symbols(
    left: PortfolioState,
    right: PortfolioState,
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                *(position.symbol.upper() for position in left.positions),
                *(position.symbol.upper() for position in right.positions),
            }
        )
    )


def is_fill_status(status: OrderStatus) -> bool:
    return status in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}
