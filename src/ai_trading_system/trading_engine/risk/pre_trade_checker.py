from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from math import isfinite

from ai_trading_system.trading_engine.config.trading_config import (
    TradingEngineConfig,
    load_trading_engine_config,
)
from ai_trading_system.trading_engine.order_sizing import resolve_order_quantity
from ai_trading_system.trading_engine.schemas.market import MarketContext
from ai_trading_system.trading_engine.schemas.order_intent import OrderIntent, OrderSide
from ai_trading_system.trading_engine.schemas.portfolio_state import PortfolioState
from ai_trading_system.trading_engine.schemas.risk_result import (
    RiskCheckResult,
    RiskRuleEvaluation,
)


class PreTradeRiskChecker:
    """Aggregates mandatory pre-trade gates before any broker submission."""

    def __init__(self, config: TradingEngineConfig | None = None) -> None:
        self.config = config or load_trading_engine_config()
        self.limits = self.config.risk_limits

    def check(
        self,
        order_intent: OrderIntent,
        portfolio_state: PortfolioState,
        market_context: MarketContext | None = None,
    ) -> RiskCheckResult:
        context = market_context or MarketContext(
            as_of=order_intent.created_at.date(),
        )
        quantity = resolve_order_quantity(order_intent)
        order_notional = float(quantity) * order_intent.limit_price
        equity = portfolio_state.equity_value_usd
        current_position = portfolio_state.position_for(order_intent.symbol)
        current_quantity = 0 if current_position is None else current_position.quantity
        current_market_value = (
            0.0 if current_position is None else current_position.market_value
        )

        evaluations = [
            self._kill_switch_rule(),
            self._membership_rule(
                "asset_type_allowed",
                order_intent.asset_type,
                self.limits.allowed_asset_types,
            ),
            self._membership_rule(
                "order_type_allowed",
                order_intent.order_type,
                self.limits.allowed_order_types,
            ),
            self._membership_rule("side_allowed", order_intent.side, self.limits.allowed_sides),
            self._short_sell_rule(order_intent, quantity, current_quantity),
            self._confidence_rule(order_intent),
            self._equity_rule(equity),
            self._order_notional_rule(order_intent, order_notional, equity),
            self._position_limit_rule(
                order_intent,
                order_notional,
                equity,
                current_market_value,
            ),
            self._total_exposure_rule(
                order_intent,
                order_notional,
                equity,
                portfolio_state.gross_exposure_usd,
                current_market_value,
            ),
            self._cash_rule(order_intent, order_notional, portfolio_state.cash_usd),
            self._duplicate_order_rule(order_intent, context),
            self._event_rule(order_intent, context),
        ]

        return RiskCheckResult.from_evaluations(
            intent_id=order_intent.intent_id,
            evaluations=evaluations,
            risk_config_version=self.config.risk_policy.version,
        )

    def _kill_switch_rule(self) -> RiskRuleEvaluation:
        passed = not self.limits.kill_switch_enabled
        return RiskRuleEvaluation(
            rule_id="kill_switch",
            passed=passed,
            actual=self.limits.kill_switch_enabled,
            limit=False,
            message="kill switch must be disabled",
        )

    def _membership_rule(
        self,
        rule_id: str,
        actual: object,
        allowed: Sequence[object],
    ) -> RiskRuleEvaluation:
        return RiskRuleEvaluation(
            rule_id=rule_id,
            passed=actual in allowed,
            actual=str(actual),
            limit=[str(item) for item in allowed],
        )

    def _short_sell_rule(
        self,
        order_intent: OrderIntent,
        quantity: int,
        current_quantity: int,
    ) -> RiskRuleEvaluation:
        would_short = order_intent.side == OrderSide.SELL and quantity > current_quantity
        return RiskRuleEvaluation(
            rule_id="short_sell_forbidden",
            passed=self.limits.allow_short or not would_short,
            actual={"requested_quantity": quantity, "current_quantity": current_quantity},
            limit={"allow_short": self.limits.allow_short},
        )

    def _confidence_rule(self, order_intent: OrderIntent) -> RiskRuleEvaluation:
        return RiskRuleEvaluation(
            rule_id="min_confidence",
            passed=order_intent.confidence >= self.limits.min_confidence,
            actual=order_intent.confidence,
            limit=self.limits.min_confidence,
        )

    def _equity_rule(self, equity: float) -> RiskRuleEvaluation:
        return RiskRuleEvaluation(
            rule_id="positive_equity",
            passed=isfinite(equity) and equity > 0,
            actual=equity,
            limit="> 0",
        )

    def _order_notional_rule(
        self,
        order_intent: OrderIntent,
        order_notional: float,
        equity: float,
    ) -> RiskRuleEvaluation:
        pct_limit = _intent_or_config_limit(
            order_intent.risk_constraints.max_order_notional_pct,
            self.limits.max_order_notional_pct,
        )
        limit_usd = equity * pct_limit if equity > 0 else 0.0
        return RiskRuleEvaluation(
            rule_id="max_order_notional_pct",
            passed=order_notional <= limit_usd,
            actual=round(order_notional, 6),
            limit=round(limit_usd, 6),
            message=f"limit_pct={pct_limit:.4f}",
        )

    def _position_limit_rule(
        self,
        order_intent: OrderIntent,
        order_notional: float,
        equity: float,
        current_market_value: float,
    ) -> RiskRuleEvaluation:
        pct_limit = _intent_or_config_limit(
            order_intent.risk_constraints.max_position_pct,
            self.limits.max_position_pct_per_symbol,
        )
        projected_value = current_market_value
        if order_intent.side == OrderSide.BUY:
            projected_value += order_notional
        else:
            projected_value = max(0.0, projected_value - order_notional)
        limit_usd = equity * pct_limit if equity > 0 else 0.0
        return RiskRuleEvaluation(
            rule_id="max_position_pct_per_symbol",
            passed=projected_value <= limit_usd,
            actual=round(projected_value, 6),
            limit=round(limit_usd, 6),
            message=f"limit_pct={pct_limit:.4f}",
        )

    def _total_exposure_rule(
        self,
        order_intent: OrderIntent,
        order_notional: float,
        equity: float,
        current_gross_exposure: float,
        current_market_value: float,
    ) -> RiskRuleEvaluation:
        projected_exposure = current_gross_exposure
        if order_intent.side == OrderSide.BUY:
            projected_exposure += order_notional
        else:
            exposure_reduction = min(order_notional, current_market_value)
            projected_exposure = max(0.0, current_gross_exposure - exposure_reduction)
        limit_usd = equity * self.limits.max_total_exposure_pct if equity > 0 else 0.0
        return RiskRuleEvaluation(
            rule_id="max_total_exposure_pct",
            passed=projected_exposure <= limit_usd,
            actual=round(projected_exposure, 6),
            limit=round(limit_usd, 6),
        )

    def _cash_rule(
        self,
        order_intent: OrderIntent,
        order_notional: float,
        cash_usd: float,
    ) -> RiskRuleEvaluation:
        if order_intent.side == OrderSide.SELL:
            return RiskRuleEvaluation(
                rule_id="cash_available",
                passed=True,
                actual=cash_usd,
                limit="not required for sell",
            )
        return RiskRuleEvaluation(
            rule_id="cash_available",
            passed=order_notional <= cash_usd,
            actual=round(order_notional, 6),
            limit=round(cash_usd, 6),
        )

    def _duplicate_order_rule(
        self,
        order_intent: OrderIntent,
        market_context: MarketContext,
    ) -> RiskRuleEvaluation:
        current_count = market_context.duplicate_count_for(order_intent.duplicate_key)
        return RiskRuleEvaluation(
            rule_id="max_daily_trades_per_symbol",
            passed=current_count < self.limits.max_daily_trades_per_symbol,
            actual=current_count,
            limit=self.limits.max_daily_trades_per_symbol,
        )

    def _event_rule(
        self,
        order_intent: OrderIntent,
        market_context: MarketContext,
    ) -> RiskRuleEvaluation:
        reason = market_context.event_blocked_symbols.get(order_intent.symbol)
        return RiskRuleEvaluation(
            rule_id="event_blackout",
            passed=reason is None,
            actual=reason or "clear",
            limit="no active event blackout",
        )


def market_context_for_date(as_of: date) -> MarketContext:
    return MarketContext(as_of=as_of)


def _intent_or_config_limit(intent_limit: float | None, config_limit: float) -> float:
    if intent_limit is None:
        return config_limit
    return min(intent_limit, config_limit)
