from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from ai_trading_system.trading_engine.schemas.broker_position import BrokerPosition
from ai_trading_system.trading_engine.schemas.order_intent import OrderSide
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState


@dataclass
class _PaperPosition:
    symbol: str
    quantity: int
    avg_cost: float


class PaperPortfolio:
    def __init__(self, initial_cash_usd: float = 100000.0) -> None:
        if initial_cash_usd <= 0:
            raise ValueError("initial_cash_usd must be positive")
        self.cash_usd = float(initial_cash_usd)
        self.realized_pnl_usd = 0.0
        self._positions: dict[str, _PaperPosition] = {}

    def apply_fill(
        self,
        *,
        symbol: str,
        side: OrderSide,
        quantity: int,
        price: float,
        fees: float,
    ) -> None:
        if quantity <= 0:
            raise ValueError("fill quantity must be positive")
        if price <= 0:
            raise ValueError("fill price must be positive")
        if fees < 0:
            raise ValueError("fees must not be negative")

        normalized = symbol.upper()
        notional = quantity * price
        position = self._positions.get(normalized)

        if side == OrderSide.BUY:
            total_cost = notional + fees
            if total_cost > self.cash_usd:
                raise ValueError("paper portfolio cash is insufficient")
            if position is None:
                self._positions[normalized] = _PaperPosition(
                    symbol=normalized,
                    quantity=quantity,
                    avg_cost=price,
                )
            else:
                new_quantity = position.quantity + quantity
                position.avg_cost = (
                    (position.avg_cost * position.quantity) + notional
                ) / new_quantity
                position.quantity = new_quantity
            self.cash_usd -= total_cost
            return

        if position is None or quantity > position.quantity:
            raise ValueError("paper portfolio cannot sell more than current position")
        self.cash_usd += notional - fees
        self.realized_pnl_usd += (price - position.avg_cost) * quantity - fees
        position.quantity -= quantity
        if position.quantity == 0:
            del self._positions[normalized]

    def position_quantity(self, symbol: str) -> int:
        position = self._positions.get(symbol.upper())
        return 0 if position is None else position.quantity

    def snapshot(
        self,
        *,
        prices: dict[str, float] | None = None,
        as_of: datetime | None = None,
    ) -> PortfolioState:
        price_map = {key.upper(): value for key, value in (prices or {}).items()}
        positions: list[BrokerPosition] = []
        gross_exposure = 0.0
        net_exposure = 0.0
        for symbol in sorted(self._positions):
            position = self._positions[symbol]
            market_price = price_map.get(symbol, position.avg_cost)
            market_value = position.quantity * market_price
            unrealized_pnl = (market_price - position.avg_cost) * position.quantity
            positions.append(
                BrokerPosition(
                    symbol=symbol,
                    quantity=position.quantity,
                    avg_cost=position.avg_cost,
                    market_price=market_price,
                    market_value=market_value,
                    unrealized_pnl=unrealized_pnl,
                )
            )
            gross_exposure += abs(market_value)
            net_exposure += market_value

        return PortfolioState(
            as_of=as_of or datetime.now(UTC),
            cash_usd=self.cash_usd,
            equity_value_usd=self.cash_usd + net_exposure,
            gross_exposure_usd=gross_exposure,
            net_exposure_usd=net_exposure,
            realized_pnl_usd=self.realized_pnl_usd,
            positions=positions,
        )
