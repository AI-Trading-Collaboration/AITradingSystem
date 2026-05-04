from __future__ import annotations

from datetime import date

from ai_trading_system.config import (
    IndustryChainConfig,
    IndustryChainNodeConfig,
    WatchlistConfig,
)
from ai_trading_system.features.market import MarketFeatureRow, MarketFeatureSet
from ai_trading_system.industry_node_state import (
    build_industry_node_heat_report,
    render_industry_node_heat_section,
)


def test_industry_node_heat_distinguishes_concentrated_and_diffuse_nodes() -> None:
    report = build_industry_node_heat_report(
        industry_chain=_industry_chain(),
        watchlist=WatchlistConfig(items=[]),
        feature_set=MarketFeatureSet(
            as_of=date(2026, 5, 4),
            rows=(
                _trend_feature("NVDA", "return_20d", 0.14),
                _trend_feature("NVDA", "above_ma_50", 1.0),
                _trend_feature("AMD", "return_20d", 0.08),
                _trend_feature("AMD", "above_ma_50", 1.0),
                _trend_feature("MSFT", "return_20d", -0.08),
                _trend_feature("MSFT", "above_ma_50", 0.0),
            ),
            warnings=(),
        ),
    )
    markdown = render_industry_node_heat_section(report)
    items = {item.node_id: item for item in report.items}

    assert report.status == "PASS"
    assert items["gpu_asic_demand"].heat_level == "hot"
    assert items["gpu_asic_demand"].coverage == 1.0
    assert items["software_and_apps"].heat_level == "cold"
    assert items["software_and_apps"].coverage == 1.0
    assert "产业链节点热度" in markdown
    assert "不能把热度视为基本面确认" in markdown
    assert "production_effect" not in markdown
    assert "生产影响：none" in markdown


def test_industry_node_heat_marks_missing_ticker_coverage() -> None:
    report = build_industry_node_heat_report(
        industry_chain=_industry_chain(),
        watchlist=WatchlistConfig(items=[]),
        feature_set=MarketFeatureSet(
            as_of=date(2026, 5, 4),
            rows=(
                _trend_feature("NVDA", "return_20d", 0.14),
                _trend_feature("NVDA", "above_ma_50", 1.0),
            ),
            warnings=(),
        ),
    )
    gpu = next(item for item in report.items if item.node_id == "gpu_asic_demand")
    apps = next(item for item in report.items if item.node_id == "software_and_apps")

    assert report.status == "PASS_WITH_WARNINGS"
    assert gpu.heat_level == "low_coverage"
    assert gpu.missing_tickers == ("AMD",)
    assert apps.heat_level == "insufficient_data"


def _industry_chain() -> IndustryChainConfig:
    return IndustryChainConfig(
        nodes=[
            IndustryChainNodeConfig(
                node_id="gpu_asic_demand",
                name="GPU/ASIC 需求",
                description="test",
                related_tickers=["NVDA", "AMD"],
                impact_horizon="short",
                cash_flow_relevance="high",
                sentiment_relevance="high",
            ),
            IndustryChainNodeConfig(
                node_id="software_and_apps",
                name="软件与应用商业化",
                description="test",
                related_tickers=["MSFT"],
                impact_horizon="long",
                cash_flow_relevance="high",
                sentiment_relevance="medium",
            ),
        ]
    )


def _trend_feature(ticker: str, feature: str, value: float) -> MarketFeatureRow:
    return MarketFeatureRow(
        as_of=date(2026, 5, 4),
        source_date=date(2026, 5, 4),
        category="trend",
        subject=ticker,
        feature=feature,
        value=value,
        unit="ratio",
        lookback=20,
        source="prices_daily",
    )
