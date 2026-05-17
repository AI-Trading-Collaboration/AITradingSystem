from __future__ import annotations

from datetime import date

from ai_trading_system.trading_engine.config import TradingEngineConfig, load_trading_engine_config
from ai_trading_system.trading_engine.portfolio import PaperPortfolio
from ai_trading_system.trading_engine.risk import PreTradeRiskChecker
from ai_trading_system.trading_engine.schemas import (
    AssetType,
    MarketContext,
    OrderIntent,
    OrderSide,
    OrderType,
    RiskCheckResult,
    TimeInForce,
)


def test_pre_trade_checker_blocks_kill_switch() -> None:
    result = _check(_intent(), config=_config(kill_switch_enabled=True))

    assert result.approved is False
    assert "kill_switch" in result.blocked_by


def test_pre_trade_checker_blocks_low_confidence() -> None:
    result = _check(_intent(confidence=0.59))

    assert result.approved is False
    assert "min_confidence" in result.blocked_by


def test_pre_trade_checker_blocks_order_notional_above_limit() -> None:
    result = _check(_intent(target_notional_usd=6000.0, limit_price=100.0))

    assert result.approved is False
    assert "max_order_notional_pct" in result.blocked_by


def test_pre_trade_checker_blocks_position_limit() -> None:
    portfolio = PaperPortfolio(100000.0)
    portfolio.apply_fill(
        symbol="TSM",
        side=OrderSide.BUY,
        quantity=90,
        price=100.0,
        fees=0.0,
    )

    result = _check(
        _intent(target_notional_usd=2000.0, limit_price=100.0),
        portfolio=portfolio,
    )

    assert result.approved is False
    assert "max_position_pct_per_symbol" in result.blocked_by


def test_pre_trade_checker_blocks_cash_shortfall() -> None:
    portfolio_state = PaperPortfolio(100000.0).snapshot()
    portfolio_state = portfolio_state.model_copy(update={"cash_usd": 1000.0})
    result = PreTradeRiskChecker(_config()).check(
        _intent(target_notional_usd=2000.0, limit_price=100.0),
        portfolio_state,
        MarketContext(as_of=date(2026, 5, 17)),
    )

    assert result.approved is False
    assert "cash_available" in result.blocked_by


def test_pre_trade_checker_approves_valid_order() -> None:
    result = _check(_intent())

    assert result.approved is True
    assert result.blocked_by == []


def test_pre_trade_checker_blocks_duplicate_daily_order() -> None:
    intent = _intent()
    context = MarketContext(
        as_of=date(2026, 5, 17),
        daily_trade_counts={intent.duplicate_key: 2},
    )

    result = PreTradeRiskChecker(_config()).check(
        intent,
        PaperPortfolio(100000.0).snapshot(),
        context,
    )

    assert result.approved is False
    assert "max_daily_trades_per_symbol" in result.blocked_by


def _check(
    intent: OrderIntent,
    *,
    config: TradingEngineConfig | None = None,
    portfolio: PaperPortfolio | None = None,
) -> RiskCheckResult:
    return PreTradeRiskChecker(config or _config()).check(
        intent,
        (portfolio or PaperPortfolio(100000.0)).snapshot(),
        MarketContext(as_of=date(2026, 5, 17)),
    )


def _config(**risk_overrides: object) -> TradingEngineConfig:
    config = load_trading_engine_config()
    risk_limits = config.risk_limits.model_copy(update=risk_overrides)
    return config.model_copy(update={"risk_limits": risk_limits})


def _intent(**overrides: object) -> OrderIntent:
    values = {
        "strategy_id": "risk_test_strategy",
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
