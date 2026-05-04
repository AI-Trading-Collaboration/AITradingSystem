from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.config import (
    IndustryChainConfig,
    IndustryChainNodeConfig,
    RiskEventLevelConfig,
    RiskEventRuleConfig,
    RiskEventsConfig,
    WatchlistConfig,
)
from ai_trading_system.features.market import MarketFeatureRow, MarketFeatureSet
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeatureRow,
    SecFundamentalFeaturesReport,
)
from ai_trading_system.fundamentals.sec_metrics import (
    SecFundamentalMetricsCsvValidationReport,
)
from ai_trading_system.industry_node_state import (
    build_industry_node_heat_report,
    render_industry_node_heat_section,
)
from ai_trading_system.risk_events import (
    RiskEventOccurrenceReviewItem,
    RiskEventOccurrenceReviewReport,
    RiskEventOccurrenceValidationReport,
)
from ai_trading_system.thesis import (
    LoadedTradeThesis,
    ThesisReviewItem,
    ThesisReviewReport,
    ThesisValidationReport,
    TradeThesis,
)
from ai_trading_system.valuation import (
    ValuationReviewItem,
    ValuationReviewReport,
    ValuationValidationReport,
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

    assert report.status == "PASS_WITH_WARNINGS"
    assert items["gpu_asic_demand"].heat_level == "hot"
    assert items["gpu_asic_demand"].health_level == "price_only"
    assert items["gpu_asic_demand"].coverage == 1.0
    assert items["software_and_apps"].heat_level == "cold"
    assert items["software_and_apps"].coverage == 1.0
    assert "产业链节点热度" in markdown
    assert "仅价格热度" in markdown
    assert "不能把热度视为基本面确认" in markdown
    assert "production_effect" not in markdown
    assert "生产影响：none" in markdown


def test_industry_node_health_uses_reviewed_inputs_without_rewriting_risks() -> None:
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
            ),
            warnings=(),
        ),
        fundamental_feature_report=_fundamental_feature_report(),
        valuation_review_report=_valuation_review_report(),
        risk_event_occurrence_review_report=_risk_event_occurrence_review_report(),
        thesis_review_report=_thesis_review_report(),
    )
    markdown = render_industry_node_heat_section(report)
    gpu = next(item for item in report.items if item.node_id == "gpu_asic_demand")

    assert report.status == "PASS_WITH_WARNINGS"
    assert gpu.health_level == "risk_limited"
    assert gpu.fundamental_coverage == 0.5
    assert gpu.valuation_coverage == 0.5
    assert gpu.risk_event_coverage == 1.0
    assert gpu.thesis_coverage == 0.5
    assert any("SEC/TSM 基本面覆盖" in item for item in gpu.support_items)
    assert any("Thesis intact" in item for item in gpu.support_items)
    assert any("估值/拥挤限制" in item for item in gpu.risk_items)
    assert any("风险事件 active/watch" in item for item in gpu.risk_items)
    assert "健康度" in markdown
    assert "不等同基本面证伪" in markdown


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


def _fundamental_feature_report() -> SecFundamentalFeaturesReport:
    as_of = date(2026, 5, 4)
    return SecFundamentalFeaturesReport(
        as_of=as_of,
        input_path=Path("sec_fundamentals.csv"),
        validation_report=SecFundamentalMetricsCsvValidationReport(
            as_of=as_of,
            input_path=Path("sec_fundamentals.csv"),
            row_count=1,
            as_of_row_count=1,
            expected_observation_count=1,
            observed_observation_count=1,
        ),
        rows=(
            SecFundamentalFeatureRow(
                as_of=as_of,
                ticker="NVDA",
                period_type="quarterly",
                fiscal_year=2026,
                fiscal_period="Q1",
                end_date=as_of,
                filed_date=as_of,
                feature_id="gross_margin",
                feature_name="Gross Margin",
                value=0.72,
                unit="ratio",
                numerator_metric_id="gross_profit",
                denominator_metric_id="revenue",
                numerator_value=720,
                denominator_value=1000,
                source_metric_accessions="0000000000-26-000001",
                source_path=Path("sec_fundamentals.csv"),
            ),
        ),
    )


def _valuation_review_report() -> ValuationReviewReport:
    as_of = date(2026, 5, 4)
    return ValuationReviewReport(
        as_of=as_of,
        validation_report=ValuationValidationReport(
            as_of=as_of,
            input_path=Path("valuation_snapshots"),
            snapshots=(),
        ),
        items=(
            ValuationReviewItem(
                snapshot_id="nvda_valuation",
                ticker="NVDA",
                source_type="manual_input",
                as_of=as_of,
                point_in_time_class="captured_snapshot",
                history_source_class="captured_snapshot_history",
                confidence_level="medium",
                confidence_reason="test",
                backtest_use="captured_at_forward_only",
                health="EXPENSIVE_OR_CROWDED",
                reason="估值偏贵或拥挤度偏高，只能作为仓位折扣信号。",
                valuation_percentile=82.0,
                overall_assessment="expensive",
                extreme_crowding_signals=(),
                elevated_crowding_signals=("valuation_percentile",),
            ),
        ),
    )


def _risk_event_occurrence_review_report() -> RiskEventOccurrenceReviewReport:
    as_of = date(2026, 5, 4)
    return RiskEventOccurrenceReviewReport(
        as_of=as_of,
        validation_report=RiskEventOccurrenceValidationReport(
            as_of=as_of,
            input_path=Path("risk_event_occurrences"),
            config=_risk_events_config(),
            occurrences=(),
        ),
        items=(
            RiskEventOccurrenceReviewItem(
                occurrence_id="gpu_margin_pressure_2026_05_04",
                event_id="gpu_margin_pressure",
                level="L2",
                status="active",
                evidence_grade="A",
                severity="high",
                probability="confirmed",
                scope="industry_chain_node",
                time_sensitivity="high",
                reversibility="partly_reversible",
                action_class="position_gate_eligible",
                reviewer="risk_owner",
                reviewed_at=as_of,
                review_decision="confirmed_active",
                next_review_due=as_of,
                triggered_at=as_of,
                last_confirmed_at=as_of,
                source_types=("manual_input",),
                target_ai_exposure_multiplier=0.7,
                score_eligible=True,
                position_gate_eligible=True,
                health="ACTIVE_L2",
                reason="测试用活跃风险事件。",
            ),
        ),
    )


def _risk_events_config() -> RiskEventsConfig:
    return RiskEventsConfig(
        levels=[
            RiskEventLevelConfig(
                level="L1",
                name="L1",
                description="low",
                default_action="monitor",
                target_ai_exposure_multiplier=1.0,
                requires_manual_review=False,
            ),
            RiskEventLevelConfig(
                level="L2",
                name="L2",
                description="medium",
                default_action="review",
                target_ai_exposure_multiplier=0.7,
                requires_manual_review=True,
            ),
            RiskEventLevelConfig(
                level="L3",
                name="L3",
                description="high",
                default_action="reduce",
                target_ai_exposure_multiplier=0.4,
                requires_manual_review=True,
            ),
        ],
        event_rules=[
            RiskEventRuleConfig(
                event_id="gpu_margin_pressure",
                name="GPU 毛利压力",
                level="L2",
                description="test",
                affected_nodes=["gpu_asic_demand"],
                related_tickers=["NVDA"],
                trigger_examples=["test"],
                recommended_actions=["manual review"],
            ),
        ],
    )


def _thesis_review_report() -> ThesisReviewReport:
    as_of = date(2026, 5, 4)
    thesis = TradeThesis(
        thesis_id="nvda_ai_demand",
        ticker="NVDA",
        direction="long",
        created_at=as_of,
        time_horizon="medium",
        position_scope="risk_asset_ai",
        entry_reason=["AI accelerator demand"],
        ai_chain_nodes=["gpu_asic_demand"],
        validation_metrics=[
            {
                "metric_id": "gross_margin",
                "description": "Gross margin remains high",
                "evidence_source": "sec_fundamentals",
                "expected_direction": "stable_or_up",
                "latest_status": "confirmed",
                "updated_at": as_of,
            }
        ],
        falsification_conditions=[
            {
                "condition_id": "gross_margin_break",
                "description": "Gross margin breaks lower",
                "severity": "high",
                "triggered": False,
            }
        ],
        review_frequency="weekly",
        status="active",
    )
    validation_report = ThesisValidationReport(
        as_of=as_of,
        input_path=Path("thesis"),
        theses=(LoadedTradeThesis(thesis=thesis, path=Path("thesis/nvda.yaml")),),
    )
    return ThesisReviewReport(
        as_of=as_of,
        validation_report=validation_report,
        items=(
            ThesisReviewItem(
                thesis_id="nvda_ai_demand",
                ticker="NVDA",
                status="active",
                health="INTACT",
                health_reason="test",
                confirmed_metrics=1,
                pending_metrics=0,
                weakened_metrics=0,
                falsified_metrics=0,
                stale_metric_ids=(),
                triggered_condition_ids=(),
                active_risk_event_ids=(),
            ),
        ),
    )
