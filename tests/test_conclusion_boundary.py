from __future__ import annotations

from ai_trading_system.conclusion_boundary import (
    classify_conclusion_boundary,
    render_conclusion_boundary_section,
)


def test_conclusion_boundary_separates_usage_level_from_posture() -> None:
    boundary = classify_conclusion_boundary(
        report_status="PASS",
        data_quality_status="PASS",
        posture_label="中高配但受限",
        confidence_level="high",
    )
    markdown = render_conclusion_boundary_section(boundary)

    assert boundary.usage_level == "actionable"
    assert boundary.posture_label == "中高配但受限"
    assert "结论等级：可作为仓位复核依据（`actionable`）" in markdown
    assert "投资姿态标签：中高配但受限" in markdown
    assert "两者不能互相替代" in markdown


def test_conclusion_boundary_downgrades_low_confidence_and_backtest_gaps() -> None:
    low_confidence = classify_conclusion_boundary(
        report_status="PASS",
        data_quality_status="PASS",
        posture_label="人工复核",
        confidence_level="low",
    )
    backtest_limited = classify_conclusion_boundary(
        report_status="PASS_WITH_LIMITATIONS",
        data_quality_status="PASS",
        posture_label="历史回测结论",
        has_backtest_limitations=True,
    )

    assert low_confidence.usage_level == "review_required"
    assert backtest_limited.usage_level == "backtest_limited"


def test_conclusion_boundary_supports_trend_judgment_scope() -> None:
    boundary = classify_conclusion_boundary(
        report_status="PASS",
        data_quality_status="PASS",
        posture_label="中高配但受限",
        confidence_level="high",
        decision_scope="trend_judgment",
    )
    markdown = render_conclusion_boundary_section(boundary)

    assert boundary.usage_level == "trend_only"
    assert boundary.decision_scope == "trend_judgment"
    assert "结论等级：趋势判断，不触发交易（`trend_only`）" in markdown
    assert "适用范围：趋势判断/投研辅助，不触发交易（`trend_judgment`）" in markdown
