from __future__ import annotations

from typing import Protocol

from ai_trading_system.trading_engine.schemas.broker_order import BrokerOrder
from ai_trading_system.trading_engine.schemas.broker_position import BrokerPosition
from ai_trading_system.trading_engine.schemas.order_intent import OrderIntent


class BrokerAdapter(Protocol):
    def submit_order(self, order_intent: OrderIntent) -> BrokerOrder:
        raise NotImplementedError

    def cancel_order(self, broker_order_id: str) -> BrokerOrder:
        raise NotImplementedError

    def get_order(self, broker_order_id: str) -> BrokerOrder:
        raise NotImplementedError

    def list_positions(self) -> list[BrokerPosition]:
        raise NotImplementedError

    def get_account_summary(self) -> dict[str, object]:
        raise NotImplementedError
