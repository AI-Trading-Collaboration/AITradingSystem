from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

from ai_trading_system.alerts import (
    build_daily_alert_report,
    render_alert_report,
    render_alert_summary_section,
)
from ai_trading_system.catalyst_calendar import (
    CatalystCalendar,
    CatalystCalendarStore,
    CatalystCalendarValidationReport,
    CatalystEvent,
)
from ai_trading_system.config import load_risk_events, load_scoring_rules
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityIssue,
    DataQualityReport,
    Severity,
)
from ai_trading_system.features.market import MarketFeatureRow, MarketFeatureSet
from ai_trading_system.risk_events import (
    LoadedRiskEventOccurrence,
    RiskEventEvidenceSource,
    RiskEventOccurrence,
    RiskEventOccurrenceValidationReport,
    build_risk_event_occurrence_review_report,
)
from ai_trading_system.scoring.daily import (
    DailyManualReviewStatus,
    DailyReviewSummary,
    PreviousDailyScoreSnapshot,
    build_daily_score_report,
    render_daily_score_report,
)
from ai_trading_system.valuation import (
    LoadedValuationSnapshot,
    SnapshotMetric,
    ValuationSnapshot,
    ValuationValidationReport,
    build_valuation_review_report,
)


def test_daily_alert_report_has_schema_refs_and_dedupe() -> None:
    as_of = date(2026, 4, 30)
    report = build_daily_score_report(
        feature_set=_feature_set_with_warning(as_of),
        data_quality_report=_quality_report_with_warning(as_of),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        valuation_review_report=_valuation_review_report(as_of),
        risk_event_occurrence_review_report=_risk_event_occurrence_review_report(as_of),
        review_summary=DailyReviewSummary(
            thesis=DailyManualReviewStatus(
                name="交易 thesis",
                status="PASS_WITH_WARNINGS",
                summary="测试 thesis 警告。",
                warning_count=1,
                source_path=Path("trade_theses/test.yaml"),
            )
        ),
    )
    previous = PreviousDailyScoreSnapshot(
        as_of=date(2026, 4, 29),
        overall_score=80.0,
        confidence_score=80.0,
        confidence_level="high",
        component_scores={},
        component_confidence_scores={},
        model_risk_asset_ai_min=0.60,
        model_risk_asset_ai_max=0.80,
        final_risk_asset_ai_min=0.60,
        final_risk_asset_ai_max=0.80,
        confidence_adjusted_risk_asset_ai_min=0.60,
        confidence_adjusted_risk_asset_ai_max=0.80,
        total_asset_ai_min=0.48,
        total_asset_ai_max=0.64,
        triggered_position_gates=None,
    )

    alert_report = build_daily_alert_report(
        report,
        previous_score_snapshot=previous,
        catalyst_calendar_report=_catalyst_calendar_report(as_of),
        data_quality_report_path=Path("outputs/reports/data_quality.md"),
        risk_event_occurrence_report_path=Path("outputs/reports/risk_events.md"),
        valuation_report_path=Path("data/external/valuation_snapshots"),
        catalyst_calendar_path=Path("config/catalyst_calendar.yaml"),
    )

    assert alert_report.status == "ACTIVE_CRITICAL"
    assert alert_report.data_system_count > 0
    assert alert_report.investment_risk_count > 0
    assert len({alert.dedupe_key for alert in alert_report.alerts}) == len(
        alert_report.alerts
    )
    assert all(alert.production_effect == "none" for alert in alert_report.alerts)
    assert all(alert.clear_condition for alert in alert_report.alerts)
    assert any(alert.category == "data_system" for alert in alert_report.alerts)
    assert any(alert.category == "investment_risk" for alert in alert_report.alerts)
    assert any(
        "daily_score:2026-04-30" in ref
        for alert in alert_report.alerts
        for ref in alert.claim_refs
    )


def test_alert_report_and_daily_summary_render_chinese_sections() -> None:
    as_of = date(2026, 4, 30)
    report = build_daily_score_report(
        feature_set=_feature_set_with_warning(as_of),
        data_quality_report=_quality_report_with_warning(as_of),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )
    alert_report = build_daily_alert_report(report)

    full_report = render_alert_report(alert_report)
    summary = render_alert_summary_section(
        alert_report,
        report_path=Path("outputs/reports/alerts_2026-04-30.md"),
    )
    daily_report = render_daily_score_report(
        report,
        data_quality_report_path=Path("quality.md"),
        feature_report_path=Path("features.md"),
        features_path=Path("features.csv"),
        scores_path=Path("scores.csv"),
        alert_summary_section=summary,
    )

    assert "# 投资与数据告警报告" in full_report
    assert "去重键" in full_report
    assert "## 告警摘要" in daily_report
    assert "production_effect=none" in daily_report


def _feature_set_with_warning(as_of: date) -> MarketFeatureSet:
    rows = [
        _feature(as_of, "trend", "SPY", "above_ma_100", 1.0),
        _feature(as_of, "trend", "SPY", "above_ma_200", 1.0),
        _feature(as_of, "trend", "QQQ", "above_ma_100", 1.0),
        _feature(as_of, "trend", "QQQ", "above_ma_200", 1.0),
        _feature(as_of, "trend", "SMH", "above_ma_100", 1.0),
        _feature(as_of, "trend", "SMH", "above_ma_200", 1.0),
        _feature(as_of, "trend", "SOXX", "above_ma_100", 1.0),
        _feature(as_of, "trend", "SOXX", "above_ma_200", 1.0),
        _feature(as_of, "trend", "AI_CORE_WATCHLIST", "above_ma_200_ratio", 0.8),
        _feature(as_of, "relative_strength", "SMH/SPY", "relative_strength_return_20d", 0.04),
        _feature(as_of, "macro_liquidity", "DGS10", "rate_change_20d", -0.05),
        _feature(as_of, "macro_liquidity", "DGS2", "rate_change_20d", 0.05),
        _feature(as_of, "trend", "DX-Y.NYB", "return_20d", -0.02),
        _feature(as_of, "risk_sentiment", "^VIX", "vix_current", 17.0),
        _feature(as_of, "risk_sentiment", "^VIX", "vix_percentile_252", 0.35),
        _feature(as_of, "trend", "^VIX", "return_5d", -0.05),
    ]
    return MarketFeatureSet(
        as_of=as_of,
        rows=tuple(rows),
        warnings=("测试特征警告",),
    )


def _quality_report_with_warning(as_of: date) -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-05-01T00:00:00Z").to_pydatetime(),
        as_of=as_of,
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=1),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1),
        expected_price_tickers=("SPY",),
        expected_rate_series=("DGS10",),
        issues=(
            DataQualityIssue(
                severity=Severity.WARNING,
                code="test_warning",
                message="测试数据质量警告。",
            ),
        ),
    )


def _risk_event_occurrence_review_report(as_of: date):
    validation_report = RiskEventOccurrenceValidationReport(
        as_of=as_of,
        input_path=Path("risk_event_occurrences"),
        config=load_risk_events(),
        occurrences=(
            LoadedRiskEventOccurrence(
                occurrence=RiskEventOccurrence(
                    occurrence_id="taiwan_geopolitical_escalation_2026_04_30",
                    event_id="taiwan_geopolitical_escalation",
                    status="active",
                    triggered_at=as_of,
                    last_confirmed_at=as_of,
                    evidence_grade="A",
                    severity="high",
                    probability="confirmed",
                    scope="ai_bucket",
                    time_sensitivity="high",
                    reversibility="partly_reversible",
                    action_class="position_gate_eligible",
                    evidence_sources=[
                        RiskEventEvidenceSource(
                            source_name="manual_policy_review",
                            source_type="manual_input",
                            captured_at=as_of,
                        )
                    ],
                    summary="测试用 L3 风险事件。",
                ),
                path=Path("risk_event_occurrences/taiwan.yaml"),
            ),
        ),
    )
    return build_risk_event_occurrence_review_report(validation_report)


def _valuation_review_report(as_of: date):
    validation_report = ValuationValidationReport(
        as_of=as_of,
        input_path=Path("valuation_snapshots"),
        snapshots=(
            LoadedValuationSnapshot(
                snapshot=ValuationSnapshot(
                    snapshot_id="nvda_valuation",
                    ticker="NVDA",
                    as_of=as_of,
                    source_type="manual_input",
                    source_name="manual_sheet",
                    captured_at=as_of,
                    valuation_metrics=[
                        SnapshotMetric(
                            metric_id="forward_pe",
                            value=36.0,
                            unit="ratio",
                            period="next_12m",
                        ),
                    ],
                    valuation_percentile=96.0,
                    overall_assessment="extreme",
                    confidence_level="low",
                    confidence_reason="测试低可信估值输入。",
                    notes="test",
                ),
                path=Path("valuation_snapshots/nvda.yaml"),
            ),
        ),
    )
    return build_valuation_review_report(validation_report)


def _catalyst_calendar_report(as_of: date) -> CatalystCalendarValidationReport:
    calendar = CatalystCalendar(
        calendar_id="test_calendar",
        version="v1",
        status="active",
        owner="project_owner",
        description="test calendar",
        source_policy="manual_review_required",
        last_reviewed_at=as_of,
        next_review_due=as_of + timedelta(days=1),
        events=[
            CatalystEvent(
                catalyst_id="nvda_earnings",
                title="NVDA earnings",
                event_type="earnings",
                event_date=as_of + timedelta(days=3),
                status="scheduled",
                importance="critical",
                related_tickers=["NVDA"],
                related_nodes=[],
                linked_thesis_ids=[],
                linked_risk_event_ids=[],
                pre_event_actions=["pre_event_review"],
                post_event_review_targets=["thesis"],
                source_name="manual_calendar",
                source_type="manual_input",
                source_url="",
                captured_at=datetime(2026, 4, 29, tzinfo=UTC),
                reviewer="owner",
                reviewed_at=as_of,
                confidence="high",
                notes="test",
            )
        ],
    )
    return CatalystCalendarValidationReport(
        as_of=as_of,
        store=CatalystCalendarStore(
            input_path=Path("config/catalyst_calendar.yaml"),
            calendar=calendar,
        ),
        issues=(),
    )


def _feature(
    as_of: date,
    category: str,
    subject: str,
    feature: str,
    value: float,
) -> MarketFeatureRow:
    return MarketFeatureRow(
        as_of=as_of,
        source_date=as_of,
        category=category,
        subject=subject,
        feature=feature,
        value=value,
        unit="ratio",
        lookback=None,
        source="test",
    )
