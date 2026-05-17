from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Protocol

from ai_trading_system.trading_engine.audit.jsonl import JsonlAuditLogger
from ai_trading_system.trading_engine.config.trading_config import (
    TradingEngineConfig,
    load_trading_engine_config,
)
from ai_trading_system.trading_engine.execution.paper_broker import PaperBroker
from ai_trading_system.trading_engine.order_sizing import resolve_order_quantity
from ai_trading_system.trading_engine.risk.pre_trade_checker import PreTradeRiskChecker
from ai_trading_system.trading_engine.schemas.broker_order import BrokerOrder
from ai_trading_system.trading_engine.schemas.execution_report import ExecutionReport
from ai_trading_system.trading_engine.schemas.market import MarketContext, MarketSnapshot
from ai_trading_system.trading_engine.schemas.order_intent import OrderIntent
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState
from ai_trading_system.trading_engine.schemas.risk_result import RiskCheckResult


class RiskChecker(Protocol):
    def check(
        self,
        order_intent: OrderIntent,
        portfolio_state: PortfolioState,
        market_context: MarketContext | None = None,
    ) -> RiskCheckResult: ...


class ExecutionService:
    """Only approved entry point for trading engine order execution."""

    def __init__(
        self,
        *,
        risk_checker: RiskChecker | None = None,
        broker: PaperBroker | None = None,
        audit_logger: JsonlAuditLogger | None = None,
        config: TradingEngineConfig | None = None,
    ) -> None:
        self.config = config or load_trading_engine_config()
        self.config.assert_paper_only()
        if broker is not None and not isinstance(broker, PaperBroker):
            raise RuntimeError(
                "ExecutionService only supports PaperBroker in this paper-only phase"
            )
        self.risk_checker = risk_checker or PreTradeRiskChecker(self.config)
        self.broker = broker or PaperBroker(
            execution_settings=self.config.execution,
        )
        self.audit_logger = audit_logger
        self._intents_by_id: dict[str, OrderIntent] = {}
        self._last_run_id: str | None = None
        self._last_strategy_id: str | None = None
        self.risk_results: list[RiskCheckResult] = []
        self.submitted_orders: list[BrokerOrder] = []
        self.execution_reports: list[ExecutionReport] = []
        self._daily_submitted_counts: dict[tuple[date, str], int] = {}

    def execute(
        self,
        order_intent: OrderIntent,
        *,
        market_context: MarketContext | None = None,
    ) -> ExecutionReport:
        self._intents_by_id[order_intent.intent_id] = order_intent
        self._last_run_id = order_intent.run_id
        self._last_strategy_id = order_intent.strategy_id
        if self.audit_logger is not None:
            self.audit_logger.log_order_intent(order_intent)

        effective_market_context = self._market_context_with_service_counts(
            order_intent,
            market_context,
        )
        portfolio_state = self.broker.get_portfolio_state(
            prices=effective_market_context.prices,
        )
        risk_result = self.risk_checker.check(
            order_intent,
            portfolio_state,
            effective_market_context,
        )
        if self.audit_logger is not None:
            self.audit_logger.log_risk_result(risk_result, order_intent=order_intent)
        self.risk_results.append(risk_result)

        requested_quantity = _safe_resolve_quantity(order_intent)
        if not risk_result.approved:
            rejected_report = ExecutionReport.rejected(
                intent_id=order_intent.intent_id,
                requested_quantity=requested_quantity,
                rejection_reason=", ".join(risk_result.blocked_by),
            )
            self.execution_reports.append(rejected_report)
            if self.audit_logger is not None:
                self.audit_logger.log_execution_report(
                    rejected_report,
                    order_intent=order_intent,
                )
            return rejected_report

        broker_order = self.broker.submit_order(order_intent)
        self.submitted_orders.append(broker_order)
        count_key = (
            effective_market_context.as_of,
            order_intent.duplicate_key,
        )
        self._daily_submitted_counts[count_key] = self._daily_submitted_counts.get(count_key, 0) + 1
        if self.audit_logger is not None:
            self.audit_logger.log_order(broker_order, order_intent=order_intent)
        submitted_report = ExecutionReport(
            intent_id=order_intent.intent_id,
            broker_order_id=broker_order.broker_order_id,
            status=broker_order.status,
            submitted_at=broker_order.submitted_at,
            requested_quantity=broker_order.quantity,
        )
        self.execution_reports.append(submitted_report)
        if self.audit_logger is not None:
            self.audit_logger.log_execution_report(
                submitted_report,
                order_intent=order_intent,
            )
        return submitted_report

    def process_market_snapshot(
        self,
        market_snapshot: MarketSnapshot | list[MarketSnapshot],
    ) -> list[ExecutionReport]:
        reports = self.broker.process_market_snapshot(market_snapshot)
        self.execution_reports.extend(reports)
        if self.audit_logger is not None:
            for report in reports:
                order_intent = self._intents_by_id.get(report.intent_id)
                if order_intent is not None:
                    self.audit_logger.log_fill(report, order_intent=order_intent)
                    self.audit_logger.log_execution_report(
                        report,
                        order_intent=order_intent,
                    )
            snapshot_prices = _snapshot_prices(market_snapshot)
            portfolio_state = self.broker.get_portfolio_state(
                prices=snapshot_prices,
                as_of=_snapshot_as_of(market_snapshot),
            )
            related_intent_ids = tuple(
                dict.fromkeys(
                    [
                        *(report.intent_id for report in reports),
                        *(order.intent_id for order in self.broker.list_open_orders()),
                    ]
                )
            )
            self.audit_logger.log_portfolio_snapshot(
                portfolio_state,
                run_id=self._last_run_id or "unknown_run",
                strategy_id=self._last_strategy_id or "unknown_strategy",
                related_intent_ids=related_intent_ids,
            )
        return reports

    def expire_day_orders(
        self,
        *,
        completed_at: datetime | None = None,
    ) -> list[ExecutionReport]:
        reports = self.broker.expire_day_orders(completed_at=completed_at)
        self.execution_reports.extend(reports)
        if self.audit_logger is not None:
            for report in reports:
                order_intent = self._intents_by_id.get(report.intent_id)
                if order_intent is not None:
                    self.audit_logger.log_execution_report(
                        report,
                        order_intent=order_intent,
                    )
        return reports

    def get_portfolio_state(
        self,
        *,
        prices: dict[str, float] | None = None,
        as_of: datetime | None = None,
    ) -> PortfolioState:
        return self.broker.get_portfolio_state(prices=prices, as_of=as_of)

    def _market_context_with_service_counts(
        self,
        order_intent: OrderIntent,
        market_context: MarketContext | None,
    ) -> MarketContext:
        context = market_context or MarketContext(as_of=order_intent.created_at.date())
        count_key = (context.as_of, order_intent.duplicate_key)
        merged_counts = dict(context.daily_trade_counts)
        merged_counts[order_intent.duplicate_key] = merged_counts.get(
            order_intent.duplicate_key, 0
        ) + self._daily_submitted_counts.get(count_key, 0)
        return context.model_copy(update={"daily_trade_counts": merged_counts})


def _safe_resolve_quantity(order_intent: OrderIntent) -> int:
    try:
        return resolve_order_quantity(order_intent)
    except ValueError:
        return 0


def _snapshot_prices(
    market_snapshot: MarketSnapshot | list[MarketSnapshot],
) -> dict[str, float]:
    snapshots = (
        [market_snapshot] if isinstance(market_snapshot, MarketSnapshot) else market_snapshot
    )
    return {snapshot.symbol: snapshot.last for snapshot in snapshots}


def _snapshot_as_of(market_snapshot: MarketSnapshot | list[MarketSnapshot]) -> datetime:
    snapshots = (
        [market_snapshot] if isinstance(market_snapshot, MarketSnapshot) else market_snapshot
    )
    if not snapshots:
        return datetime.now(UTC)
    return max(snapshot.timestamp for snapshot in snapshots)


def market_context_from_prices(as_of: date, prices: dict[str, float]) -> MarketContext:
    return MarketContext(as_of=as_of, prices=prices)
