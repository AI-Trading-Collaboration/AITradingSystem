from __future__ import annotations

from ai_trading_system.trading_engine.schemas.broker_order import BrokerOrder
from ai_trading_system.trading_engine.schemas.broker_position import BrokerPosition
from ai_trading_system.trading_engine.schemas.order_intent import OrderIntent


class IbkrAdapterStub:
    def submit_order(self, order_intent: OrderIntent) -> BrokerOrder:
        raise RuntimeError("Real trading is not implemented in this phase")

    def cancel_order(self, broker_order_id: str) -> BrokerOrder:
        raise RuntimeError("Real trading is not implemented in this phase")

    def get_order(self, broker_order_id: str) -> BrokerOrder:
        raise RuntimeError("Real trading is not implemented in this phase")

    def list_positions(self) -> list[BrokerPosition]:
        raise RuntimeError("Real trading is not implemented in this phase")

    def get_account_summary(self) -> dict[str, object]:
        raise RuntimeError("Real trading is not implemented in this phase")
