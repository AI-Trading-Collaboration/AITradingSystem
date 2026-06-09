from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.backtest.engine import summarize_long_only_backtest
from ai_trading_system.etf_portfolio.allocation import allocate_portfolio, weights_from_records
from ai_trading_system.etf_portfolio.backtest import calculate_portfolio_accounting_step
from ai_trading_system.etf_portfolio.models import ETFQualityReport, load_etf_config_bundle

TOY_PRICE_PATH = Path(__file__).parent / "fixtures" / "etf_portfolio" / "toy_prices.csv"
TOY_SYMBOLS = ("SPY", "QQQ", "CASH")


def test_toy_single_asset_nav_uses_execution_to_return_window() -> None:
    close_pivot = _toy_close_pivot()

    step = calculate_portfolio_accounting_step(
        close_pivot,
        signal_date=date(2026, 1, 2),
        execution_date=date(2026, 1, 5),
        return_date=date(2026, 1, 6),
        target_weights={"SPY": 1.0, "QQQ": 0.0, "CASH": 0.0},
        previous_weights=None,
        asset_symbols=TOY_SYMBOLS,
        total_cost_bps=0.0,
    )

    assert step.period_returns["SPY"] == pytest.approx(-0.10)
    assert step.strategy_return == pytest.approx(-0.10)
    assert step.ending_equity == pytest.approx(0.90)


def test_toy_two_asset_rebalance_cash_cost_and_contributions_are_hand_verifiable() -> None:
    close_pivot = _toy_close_pivot()
    first_weights = {"SPY": 0.40, "QQQ": 0.50, "CASH": 0.10}
    second_weights = {"SPY": 0.20, "QQQ": 0.70, "CASH": 0.10}

    first = calculate_portfolio_accounting_step(
        close_pivot,
        signal_date=date(2026, 1, 2),
        execution_date=date(2026, 1, 5),
        return_date=date(2026, 1, 6),
        target_weights=first_weights,
        previous_weights=None,
        asset_symbols=TOY_SYMBOLS,
        total_cost_bps=10.0,
    )
    second = calculate_portfolio_accounting_step(
        close_pivot,
        signal_date=date(2026, 1, 5),
        execution_date=date(2026, 1, 6),
        return_date=date(2026, 1, 7),
        target_weights=second_weights,
        previous_weights=first_weights,
        asset_symbols=TOY_SYMBOLS,
        total_cost_bps=10.0,
        starting_equity=first.ending_equity,
    )

    assert first.asset_contributions == pytest.approx({"SPY": -0.04, "QQQ": 0.05, "CASH": 0.0})
    assert first.gross_return == pytest.approx(0.01)
    assert first.turnover == pytest.approx(0.90)
    assert first.transaction_cost == pytest.approx(0.0009)
    assert first.strategy_return == pytest.approx(0.0091)
    assert first.ending_equity == pytest.approx(1.0091)

    assert second.asset_contributions == pytest.approx({"SPY": 0.0, "QQQ": 0.07, "CASH": 0.0})
    assert second.turnover == pytest.approx(0.40)
    assert second.transaction_cost == pytest.approx(0.0004)
    assert second.strategy_return == pytest.approx(0.0696)
    assert second.ending_equity == pytest.approx(1.0091 * 1.0696)


def test_toy_accounting_rejects_same_day_execution_and_bad_weight_sum() -> None:
    close_pivot = _toy_close_pivot()
    valid_weights = {"SPY": 1.0, "QQQ": 0.0, "CASH": 0.0}

    with pytest.raises(ValueError, match="execution_date"):
        calculate_portfolio_accounting_step(
            close_pivot,
            signal_date=date(2026, 1, 2),
            execution_date=date(2026, 1, 2),
            return_date=date(2026, 1, 5),
            target_weights=valid_weights,
            previous_weights=None,
            asset_symbols=TOY_SYMBOLS,
            total_cost_bps=0.0,
        )
    with pytest.raises(ValueError, match="return_date"):
        calculate_portfolio_accounting_step(
            close_pivot,
            signal_date=date(2026, 1, 2),
            execution_date=date(2026, 1, 5),
            return_date=date(2026, 1, 5),
            target_weights=valid_weights,
            previous_weights=None,
            asset_symbols=TOY_SYMBOLS,
            total_cost_bps=0.0,
        )
    with pytest.raises(ValueError, match="sum to 1.0"):
        calculate_portfolio_accounting_step(
            close_pivot,
            signal_date=date(2026, 1, 2),
            execution_date=date(2026, 1, 5),
            return_date=date(2026, 1, 6),
            target_weights={"SPY": 0.50, "QQQ": 0.40, "CASH": 0.0},
            previous_weights=None,
            asset_symbols=TOY_SYMBOLS,
            total_cost_bps=0.0,
        )


def test_toy_portfolio_drawdown_is_hand_verifiable() -> None:
    returns = [0.10, -0.10, 0.0]
    metrics = summarize_long_only_backtest(returns, exposures=[1.0] * 3, turnovers=[0.0] * 3)

    assert metrics.max_drawdown == pytest.approx(-0.10)


def test_toy_rebalance_delta_threshold_clips_small_weight_changes() -> None:
    config = load_etf_config_bundle()
    signals = pd.DataFrame(
        [
            {"symbol": "SPY", "composite_score": 50.0},
            {"symbol": "QQQ", "composite_score": 50.0},
            {"symbol": "SMH", "composite_score": 50.0},
            {"symbol": "SOXX", "composite_score": 50.0},
        ]
    )
    previous_weights = {
        "SPY": 0.301,
        "QQQ": 0.399,
        "SMH": 0.150,
        "SOXX": 0.000,
        "CASH": 0.150,
    }

    allocation = allocate_portfolio(
        signals,
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime="Risk-On",
        run_date=date(2026, 1, 2),
        config_hash=config.config_hash,
        data_quality_report=_quality_report(),
        previous_weights=previous_weights,
    )
    weights = weights_from_records(allocation)

    assert weights["SPY"] == pytest.approx(previous_weights["SPY"])
    assert weights["QQQ"] == pytest.approx(previous_weights["QQQ"])
    assert sum(weights.values()) == pytest.approx(1.0)


def _toy_close_pivot() -> pd.DataFrame:
    frame = pd.read_csv(TOY_PRICE_PATH)
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    return frame.pivot(index="_date", columns="symbol", values="_price").sort_index()


def _quality_report() -> ETFQualityReport:
    return ETFQualityReport(
        checked_at=datetime.now(UTC),
        as_of=date(2026, 1, 2),
        status="PASS",
        row_count=15,
        symbols=TOY_SYMBOLS,
        min_date=date(2026, 1, 2),
        max_date=date(2026, 1, 8),
        checksum="toy",
    )
