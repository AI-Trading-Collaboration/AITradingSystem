from __future__ import annotations

from datetime import UTC, datetime

from ai_trading_system.trading_engine.config.trading_config import ExecutionSettings
from ai_trading_system.trading_engine.execution.fill_simulator import simulate_limit_fill
from ai_trading_system.trading_engine.execution.order_state_machine import assert_transition
from ai_trading_system.trading_engine.order_sizing import resolve_order_quantity
from ai_trading_system.trading_engine.portfolio.paper_portfolio import PaperPortfolio
from ai_trading_system.trading_engine.schemas.broker_order import BrokerOrder, OrderStatus
from ai_trading_system.trading_engine.schemas.execution_report import ExecutionReport
from ai_trading_system.trading_engine.schemas.market import MarketSnapshot
from ai_trading_system.trading_engine.schemas.order_intent import OrderIntent
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState


class PaperBroker:
    def __init__(
        self,
        *,
        portfolio: PaperPortfolio | None = None,
        execution_settings: ExecutionSettings | None = None,
    ) -> None:
        self.execution_settings = execution_settings or ExecutionSettings(
            default_initial_cash_usd=100000.0,
        )
        self.portfolio = portfolio or PaperPortfolio(
            self.execution_settings.default_initial_cash_usd,
        )
        self._orders: dict[str, BrokerOrder] = {}
        self._order_sequence = 0

    def submit_order(self, order_intent: OrderIntent) -> BrokerOrder:
        quantity = resolve_order_quantity(order_intent)
        self._order_sequence += 1
        order = BrokerOrder(
            broker_order_id=f"paper_order_{self._order_sequence:06d}",
            client_order_id=order_intent.intent_id,
            intent_id=order_intent.intent_id,
            symbol=order_intent.symbol,
            side=order_intent.side,
            order_type=order_intent.order_type,
            limit_price=order_intent.limit_price,
            quantity=quantity,
            status=OrderStatus.SUBMITTED,
            submitted_at=datetime.now(UTC),
        )
        self._orders[order.broker_order_id] = order
        return order

    def cancel_order(self, broker_order_id: str) -> BrokerOrder:
        order = self.get_order(broker_order_id)
        assert_transition(order.status, OrderStatus.CANCELLED)
        updated = order.model_copy(
            update={
                "status": OrderStatus.CANCELLED,
                "completed_at": datetime.now(UTC),
            }
        )
        self._orders[broker_order_id] = updated
        return updated

    def get_order(self, broker_order_id: str) -> BrokerOrder:
        try:
            return self._orders[broker_order_id]
        except KeyError as exc:
            raise KeyError(f"unknown paper order: {broker_order_id}") from exc

    def list_open_orders(self) -> list[BrokerOrder]:
        return [
            order
            for order in self._orders.values()
            if order.status in {OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED}
        ]

    def get_portfolio_state(
        self,
        *,
        prices: dict[str, float] | None = None,
        as_of: datetime | None = None,
    ) -> PortfolioState:
        return self.portfolio.snapshot(prices=prices, as_of=as_of)

    def process_market_snapshot(
        self,
        market_snapshot: MarketSnapshot | list[MarketSnapshot],
    ) -> list[ExecutionReport]:
        snapshots = (
            [market_snapshot]
            if isinstance(market_snapshot, MarketSnapshot)
            else market_snapshot
        )
        snapshot_by_symbol = {snapshot.symbol: snapshot for snapshot in snapshots}
        reports: list[ExecutionReport] = []
        for order in list(self.list_open_orders()):
            snapshot = snapshot_by_symbol.get(order.symbol)
            if snapshot is None:
                continue
            fill = simulate_limit_fill(
                order=order,
                snapshot=snapshot,
                commission_bps=self.execution_settings.commission_bps,
                slippage_bps=self.execution_settings.slippage_bps,
            )
            if fill is None:
                continue
            completed_at = datetime.now(UTC)
            self.portfolio.apply_fill(
                symbol=order.symbol,
                side=order.side,
                quantity=fill.quantity,
                price=fill.price,
                fees=fill.fees,
            )
            updated = order.model_copy(
                update={
                    "status": OrderStatus.FILLED,
                    "completed_at": completed_at,
                    "filled_quantity": fill.quantity,
                    "avg_fill_price": fill.price,
                }
            )
            self._orders[order.broker_order_id] = updated
            reports.append(
                ExecutionReport(
                    intent_id=order.intent_id,
                    broker_order_id=order.broker_order_id,
                    status=OrderStatus.FILLED,
                    submitted_at=order.submitted_at,
                    completed_at=completed_at,
                    requested_quantity=order.quantity,
                    filled_quantity=fill.quantity,
                    avg_fill_price=fill.price,
                    fees=fill.fees,
                    slippage_bps=fill.slippage_bps,
                )
            )
        return reports
