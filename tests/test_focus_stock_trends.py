from __future__ import annotations

from datetime import date

from ai_trading_system.config import load_watchlist
from ai_trading_system.features.market import MarketFeatureRow, MarketFeatureSet
from ai_trading_system.focus_stock_trends import (
    build_focus_stock_trend_report,
    render_focus_stock_trend_section,
)


def test_focus_stock_trend_report_classifies_core_tickers() -> None:
    feature_set = MarketFeatureSet(
        as_of=date(2026, 4, 30),
        rows=(
            *_ticker_rows(
                "NVDA",
                returns=(0.01, 0.03, 0.10),
                above_ma=(1.0, 1.0, 1.0, 1.0),
                pct_vs_ma=(0.05, 0.12),
            ),
            *_ticker_rows(
                "AMD",
                returns=(-0.01, -0.02, -0.06),
                above_ma=(0.0, 0.0, 1.0, 1.0),
                pct_vs_ma=(-0.03, 0.02),
            ),
            _feature("INTC", "return_1d", -0.01),
        ),
        warnings=(),
    )

    report = build_focus_stock_trend_report(
        feature_set=feature_set,
        tickers=("NVDA", "AMD", "INTC"),
        watchlist=load_watchlist(),
    )
    markdown = render_focus_stock_trend_section(report)
    status_by_ticker = {item.ticker: item.trend_status for item in report.items}

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.production_effect == "none"
    assert status_by_ticker["NVDA"] == "uptrend"
    assert status_by_ticker["AMD"] == "weakening"
    assert status_by_ticker["INTC"] == "data_gap"
    assert "NVIDIA" in markdown
    assert "多头延续" in markdown
    assert "转弱" in markdown
    assert "数据缺口" in markdown
    assert "不改变评分、仓位闸门、执行建议或 prediction ledger" in markdown


def _ticker_rows(
    ticker: str,
    *,
    returns: tuple[float, float, float],
    above_ma: tuple[float, float, float, float],
    pct_vs_ma: tuple[float, float],
) -> tuple[MarketFeatureRow, ...]:
    return (
        _feature(ticker, "return_1d", returns[0]),
        _feature(ticker, "return_5d", returns[1]),
        _feature(ticker, "return_20d", returns[2]),
        _feature(ticker, "above_ma_20", above_ma[0]),
        _feature(ticker, "above_ma_50", above_ma[1]),
        _feature(ticker, "above_ma_100", above_ma[2]),
        _feature(ticker, "above_ma_200", above_ma[3]),
        _feature(ticker, "pct_vs_ma_50", pct_vs_ma[0]),
        _feature(ticker, "pct_vs_ma_200", pct_vs_ma[1]),
    )


def _feature(ticker: str, feature: str, value: float) -> MarketFeatureRow:
    return MarketFeatureRow(
        as_of=date(2026, 4, 30),
        source_date=date(2026, 4, 30),
        category="trend",
        subject=ticker,
        feature=feature,
        value=value,
        unit="ratio",
        lookback=None,
        source="test",
    )
