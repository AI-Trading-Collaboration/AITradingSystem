from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pandas as pd
from typer.testing import CliRunner

import ai_trading_system.cli as cli_module
from ai_trading_system.belief_state import (
    build_belief_state,
    render_belief_state_summary,
)
from ai_trading_system.cli import app
from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_industry_chain,
    load_portfolio,
    load_risk_events,
    load_scoring_rules,
    load_sec_companies,
    load_universe,
    load_watchlist,
)
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.features.market import MarketFeatureRow, MarketFeatureSet
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeatureRow,
    SecFundamentalFeaturesReport,
)
from ai_trading_system.fundamentals.sec_metrics import (
    SEC_FUNDAMENTAL_METRIC_COLUMNS,
    PeriodType,
    SecFundamentalMetricRow,
    SecFundamentalMetricsCsvValidationReport,
    load_sec_fundamental_metric_rows_csv,
    write_sec_fundamental_metric_rows_csv,
)
from ai_trading_system.fundamentals.tsm_ir import (
    TsmIrQuarterlyMetricRow,
    tsm_ir_quarterly_metric_rows_to_frame,
)
from ai_trading_system.portfolio_exposure import (
    PortfolioExposureReport,
    build_portfolio_exposure_report,
)
from ai_trading_system.risk_events import (
    LoadedRiskEventOccurrence,
    LoadedRiskEventReviewAttestation,
    RiskEventEvidenceSource,
    RiskEventOccurrence,
    RiskEventOccurrenceReviewReport,
    RiskEventOccurrenceValidationReport,
    RiskEventReviewAttestation,
    RiskEventReviewAttestationSource,
    build_risk_event_occurrence_review_report,
)
from ai_trading_system.scoring.daily import (
    DailyManualReviewStatus,
    DailyReviewSummary,
    build_daily_score_report,
    load_previous_daily_score_snapshot,
    render_daily_score_report,
    write_scores_csv,
)
from ai_trading_system.valuation import (
    LoadedValuationSnapshot,
    SnapshotMetric,
    ValuationReviewReport,
    ValuationSnapshot,
    ValuationValidationReport,
    build_valuation_review_report,
)


def test_build_daily_score_report_uses_hard_data_and_placeholders() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )

    assert report.recommendation.total_score > 50
    assert _component(report, "trend").source_type == "hard_data"
    assert _component(report, "fundamentals").source_type == "placeholder"
    assert report.status == "PASS_WITH_LIMITATIONS"
    assert report.recommendation.total_asset_ai_band.min_position >= 0.24


def test_build_daily_score_report_uses_sec_fundamental_features() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        fundamental_feature_report=_fundamental_feature_report(),
    )

    fundamentals = _component(report, "fundamentals")

    assert fundamentals.source_type == "hard_data"
    assert fundamentals.coverage == 1.0
    assert fundamentals.score > 50
    assert any(
        signal.subject == "AI_CORE_MEDIAN"
        and signal.feature == "gross_margin_quarterly_median"
        for signal in fundamentals.signals
    )


def test_build_daily_score_report_uses_valuation_snapshots() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        valuation_review_report=_valuation_review_report(),
    )

    valuation = _component(report, "valuation")

    assert valuation.source_type == "manual_input"
    assert valuation.coverage == 1.0
    assert valuation.score < 50


def test_valuation_crowding_gate_caps_position_without_thesis_break() -> None:
    rules = load_scoring_rules()
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=rules,
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        valuation_review_report=_valuation_review_report(
            valuation_percentile=96.0,
            overall_assessment="extreme",
        ),
    )

    valuation_gate = _position_gate(report, "valuation")
    valuation = _component(report, "valuation")

    assert valuation_gate.triggered
    assert (
        valuation_gate.max_position
        == rules.position_gates.valuation.extreme_overheated_max_position
    )
    assert report.recommendation.risk_asset_ai_band.max_position <= valuation_gate.max_position
    assert valuation.source_type == "manual_input"
    assert valuation.score < 50


def test_confidence_gate_caps_position_when_no_stricter_gate() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )

    confidence = report.confidence_assessment
    confidence_gate = _position_gate(report, "confidence")

    assert confidence.level == "medium"
    assert confidence_gate.triggered
    assert confidence_gate.max_position == (
        confidence.adjusted_risk_asset_ai_band.max_position
    )
    assert report.recommendation.risk_asset_ai_band.max_position == (
        confidence.adjusted_risk_asset_ai_band.max_position
    )
    assert report.recommendation.risk_asset_ai_band.max_position < (
        report.recommendation.model_risk_asset_ai_band.max_position
    )


def test_valuation_gate_can_remain_stricter_than_confidence_gate() -> None:
    rules = load_scoring_rules()
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=rules,
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        valuation_review_report=_valuation_review_report(
            valuation_percentile=96.0,
            overall_assessment="extreme",
        ),
    )

    confidence = report.confidence_assessment
    confidence_gate = _position_gate(report, "confidence")
    valuation_gate = _position_gate(report, "valuation")

    assert confidence_gate.triggered
    assert valuation_gate.triggered
    assert valuation_gate.max_position < confidence_gate.max_position
    assert report.recommendation.risk_asset_ai_band.max_position == (
        rules.position_gates.valuation.extreme_overheated_max_position
    )
    assert report.recommendation.risk_asset_ai_band.max_position <= (
        confidence.adjusted_risk_asset_ai_band.max_position
    )


def test_build_daily_score_report_uses_risk_event_occurrences() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        risk_event_occurrence_review_report=_risk_event_occurrence_review_report(),
    )

    policy = _component(report, "policy_geopolitics")

    assert policy.source_type == "manual_input"
    assert policy.coverage == 1.0
    assert policy.score < 50
    assert report.recommendation.model_risk_asset_ai_band.max_position > (
        report.recommendation.risk_asset_ai_band.max_position
    )
    risk_gate = _position_gate(report, "risk_events")
    assert risk_gate.triggered
    assert risk_gate.max_position < report.recommendation.model_risk_asset_ai_band.max_position
    assert any(
        signal.subject == "POLICY_GEOPOLITICS"
        and signal.feature == "active_or_watch_l3_count"
        and signal.value == 1.0
        for signal in policy.signals
    )


def test_build_daily_score_report_keeps_b_grade_risk_events_out_of_gate() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        risk_event_occurrence_review_report=_risk_event_occurrence_review_report(
            evidence_grade="B",
            action_class="position_gate_eligible",
        ),
    )

    policy = _component(report, "policy_geopolitics")
    risk_gate = _position_gate(report, "risk_events")

    assert policy.source_type == "manual_input"
    assert policy.score < 50
    assert risk_gate.triggered is False
    assert risk_gate.max_position == 1.0


def test_build_daily_score_report_keeps_watch_risk_events_out_of_score_and_gate() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        risk_event_occurrence_review_report=_risk_event_occurrence_review_report(
            status="watch",
            action_class="position_gate_eligible",
        ),
    )

    policy = _component(report, "policy_geopolitics")
    risk_gate = _position_gate(report, "risk_events")

    assert policy.source_type == "insufficient_data"
    assert policy.score == 50
    assert risk_gate.triggered is False
    assert risk_gate.max_position == 1.0
    assert not any(signal.value == 1.0 for signal in policy.signals)


def test_build_daily_score_report_uses_current_risk_event_review_attestation() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        risk_event_occurrence_review_report=(
            _empty_risk_event_occurrence_review_report_with_attestation()
        ),
    )

    policy = _component(report, "policy_geopolitics")
    risk_gate = _position_gate(report, "risk_events")

    assert policy.source_type == "manual_input"
    assert policy.coverage == 1.0
    assert policy.score == 100.0
    assert policy.confidence == 0.75
    assert "已完成覆盖评估日" in policy.reason
    assert risk_gate.triggered is False
    assert risk_gate.max_position == 1.0


def test_build_daily_score_report_labels_llm_formal_assessment_attestation() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        risk_event_occurrence_review_report=(
            _empty_risk_event_occurrence_review_report_with_llm_formal_attestation()
        ),
    )

    policy = _component(report, "policy_geopolitics")

    assert policy.source_type == "llm_formal_assessment"
    assert policy.coverage == 1.0
    assert policy.score == 100.0
    assert policy.confidence == 0.65
    assert "LLM formal assessment" in policy.reason
    assert "低置信度模块：政策/地缘" not in "；".join(
        report.confidence_assessment.reasons
    )


def test_risk_budget_gate_caps_high_vix_market_stress() -> None:
    portfolio = load_portfolio()
    report = build_daily_score_report(
        feature_set=_feature_set_with_vix(vix_current=34.0, vix_percentile=0.90),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        max_total_ai_exposure=portfolio.position_limits.max_total_ai_exposure,
        risk_budget=portfolio.risk_budget,
    )

    risk_budget_gate = _position_gate(report, "risk_budget")

    assert risk_budget_gate.triggered
    assert (
        risk_budget_gate.max_position
        == portfolio.risk_budget.market_stress.stress_max_position
    )
    assert "市场压力达到 stress 阈值" in risk_budget_gate.reason
    assert report.recommendation.risk_asset_ai_band.max_position <= (
        risk_budget_gate.max_position
    )


def test_risk_budget_gate_caps_real_portfolio_concentration(
    tmp_path: Path,
) -> None:
    portfolio = load_portfolio()
    exposure_report = _concentrated_portfolio_exposure_report(tmp_path)
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        max_total_ai_exposure=portfolio.position_limits.max_total_ai_exposure,
        risk_budget=portfolio.risk_budget,
        portfolio_exposure_report=exposure_report,
    )

    risk_budget_gate = _position_gate(report, "risk_budget")

    assert exposure_report.status == "PASS"
    assert risk_budget_gate.triggered
    assert (
        risk_budget_gate.max_position
        == portfolio.risk_budget.concentration.concentration_max_position
    )
    assert "单票 AI 暴露集中度" in risk_budget_gate.reason


def test_macro_risk_asset_budget_cuts_total_budget_not_ai_relative_weight() -> None:
    portfolio = load_portfolio()
    report = build_daily_score_report(
        feature_set=_feature_set_with_macro_stress(
            dgs10_rate_change_20d=0.55,
            dollar_return_20d=0.01,
        ),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
        macro_risk_asset_budget=portfolio.macro_risk_asset_budget,
        fundamental_feature_report=_fundamental_feature_report(),
        risk_event_occurrence_review_report=(
            _empty_risk_event_occurrence_review_report_with_attestation()
        ),
    )

    adjustment = report.macro_risk_asset_budget

    assert adjustment.triggered
    assert adjustment.level == "stress"
    assert (
        report.recommendation.total_risk_asset_band.min_position
        == portfolio.macro_risk_asset_budget.stress_total_risk_asset_min
    )
    assert (
        report.recommendation.total_risk_asset_band.max_position
        == portfolio.macro_risk_asset_budget.stress_total_risk_asset_max
    )
    assert (
        report.recommendation.risk_asset_ai_band.max_position
        == report.recommendation.model_risk_asset_ai_band.max_position
    )
    assert report.recommendation.total_asset_ai_band.max_position == (
        report.recommendation.risk_asset_ai_band.max_position
        * portfolio.macro_risk_asset_budget.stress_total_risk_asset_max
    )


def test_build_daily_score_report_marks_insufficient_data() -> None:
    report = build_daily_score_report(
        feature_set=MarketFeatureSet(
            as_of=date(2026, 4, 30),
            rows=(
                MarketFeatureRow(
                    as_of=date(2026, 4, 30),
                    source_date=date(2026, 4, 30),
                    category="risk_sentiment",
                    subject="^VIX",
                    feature="vix_current",
                    value=18.0,
                    unit="index_level",
                    lookback=None,
                    source="prices_daily",
                ),
            ),
            warnings=(),
        ),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )

    assert _component(report, "trend").source_type == "insufficient_data"
    assert _component(report, "trend").score == 50


def test_write_scores_csv_upserts_as_of_rows(tmp_path: Path) -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )
    output_path = tmp_path / "scores_daily.csv"

    write_scores_csv(report, output_path)
    write_scores_csv(report, output_path)

    stored = pd.read_csv(output_path)

    assert set(stored["as_of"]) == {"2026-04-30"}
    assert len(stored) == len(report.components) + 1
    assert {"confidence", "confidence_level", "confidence_reasons"}.issubset(
        stored.columns
    )
    assert {
        "model_risk_asset_ai_min",
        "model_risk_asset_ai_max",
        "final_risk_asset_ai_min",
        "final_risk_asset_ai_max",
        "confidence_adjusted_risk_asset_ai_min",
        "confidence_adjusted_risk_asset_ai_max",
        "total_asset_ai_min",
        "total_asset_ai_max",
        "static_total_risk_asset_min",
        "static_total_risk_asset_max",
        "final_total_risk_asset_min",
        "final_total_risk_asset_max",
        "macro_risk_asset_budget_level",
        "macro_risk_asset_budget_triggered",
        "macro_risk_asset_budget_reasons",
        "triggered_position_gates",
    }.issubset(stored.columns)
    overall = stored.loc[stored["component"] == "overall"].iloc[0]
    assert overall["confidence_level"] in {"high", "medium", "low"}
    assert overall["confidence_reasons"]
    assert overall["final_risk_asset_ai_max"] == report.recommendation.max_position


def test_daily_score_confidence_is_reported_separately_from_market_score() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )

    confidence = report.confidence_assessment

    assert report.recommendation.total_score > 50
    assert 0 <= confidence.score <= 100
    assert confidence.level in {"high", "medium", "low"}
    assert any("低置信度模块" in reason for reason in confidence.reasons)
    assert confidence.adjusted_risk_asset_ai_band.max_position <= (
        report.recommendation.model_risk_asset_ai_band.max_position
    )
    assert report.recommendation.risk_asset_ai_band.max_position == (
        confidence.adjusted_risk_asset_ai_band.max_position
    )
    assert _position_gate(report, "confidence").max_position == (
        confidence.adjusted_risk_asset_ai_band.max_position
    )


def test_load_previous_daily_score_snapshot_reads_latest_prior_overall(
    tmp_path: Path,
) -> None:
    scores_path = tmp_path / "scores_daily.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-04-29",
                "component": "overall",
                "score": 55.0,
                "confidence": 61.0,
                "confidence_level": "medium",
                "final_risk_asset_ai_min": 0.40,
                "final_risk_asset_ai_max": 0.60,
            },
            {
                "as_of": "2026-04-30",
                "component": "overall",
                "score": 65.0,
                "confidence": 75.0,
                "confidence_level": "high",
                "final_risk_asset_ai_min": 0.60,
                "final_risk_asset_ai_max": 0.80,
            },
            {
                "as_of": "2026-04-30",
                "component": "trend",
                "score": 70.0,
                "confidence": 0.9,
            },
        ]
    ).to_csv(scores_path, index=False)

    snapshot = load_previous_daily_score_snapshot(scores_path, date(2026, 5, 1))

    assert snapshot is not None
    assert snapshot.as_of == date(2026, 4, 30)
    assert snapshot.overall_score == 65.0
    assert snapshot.confidence_level == "high"
    assert snapshot.final_risk_asset_ai_max == 0.80
    assert snapshot.component_scores["trend"] == 70.0
    assert snapshot.component_confidence_scores["trend"] == 0.9


def test_render_daily_score_report_includes_data_gate_and_limitations(tmp_path: Path) -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        review_summary=DailyReviewSummary(
            thesis=DailyManualReviewStatus(
                name="交易 thesis",
                status="PASS_WITH_WARNINGS",
                summary="Thesis 1 个，活跃 1 个；需关注 1 个，已证伪 0 个。",
                warning_count=1,
                source_path=tmp_path / "trade_theses",
            ),
            risk_events=DailyManualReviewStatus(
                name="风险事件",
                status="PASS",
                summary="风险规则 3 条，活跃 1 条；活跃 L2/L3 规则 0 条。",
                source_path=tmp_path / "risk_events.yaml",
            ),
        ),
    )
    scores_path = tmp_path / "scores.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-04-29",
                "component": "overall",
                "score": 50.0,
                "confidence": 55.0,
                "final_risk_asset_ai_min": 0.40,
                "final_risk_asset_ai_max": 0.60,
            },
            {
                "as_of": "2026-04-29",
                "component": "trend",
                "score": 60.0,
                "confidence": 0.80,
            },
        ]
    ).to_csv(scores_path, index=False)
    previous = load_previous_daily_score_snapshot(scores_path, date(2026, 4, 30))

    markdown = render_daily_score_report(
        report,
        data_quality_report_path=tmp_path / "quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features.csv",
        scores_path=scores_path,
        previous_score_snapshot=previous,
        risk_event_openai_precheck_section=(
            "## 日报前 OpenAI 风险事件预审\n\n"
            "- OpenAI 预审状态：PASS\n"
            "- 待人工复核队列记录数：0"
        ),
    )

    assert "- 数据质量状态：PASS" in markdown
    assert "## 今日结论卡" in markdown
    assert "| Data Gate | PASS；市场和宏观缓存质量门禁通过。 |" in markdown
    assert "| Run ID / Trace | 未传入 run_id |" in markdown
    assert "### Main Invalidator" in markdown
    assert "### Next Checks" in markdown
    assert "## Base Signal / Risk Caps" in markdown
    assert "Base signal score" in markdown
    assert "### Risk Caps" in markdown
    assert "`score_mapping`" in markdown
    assert "### Channel Audit" in markdown
    assert "`alpha`" in markdown
    assert "## 结论使用等级" in markdown
    assert "适用范围：趋势判断/投研辅助，不触发交易（`trend_judgment`）" in markdown
    assert "结论等级：必须人工复核（`review_required`）" in markdown
    assert "### 三个核心原因" in markdown
    assert "### 最大限制" in markdown
    assert "### 下一步触发条件" in markdown
    assert "## 人工复核摘要" in markdown
    assert "## 日报前 OpenAI 风险事件预审" in markdown
    assert "- 待人工复核队列记录数：0" in markdown
    assert "## 宏观风险资产预算" in markdown
    assert "## 仓位闸门" in markdown
    assert "## 变化原因树" in markdown
    assert markdown.index("## 今日结论卡") < markdown.index("## 变化原因树")
    assert markdown.index("## 今日结论卡") < markdown.index("## Base Signal / Risk Caps")
    assert markdown.index("## Base Signal / Risk Caps") < markdown.index("## 结论使用等级")
    assert markdown.index("## 结论使用等级") < markdown.index("## 变化原因树")
    assert markdown.index("## 今日结论卡") < markdown.index("## 数据门禁")
    assert markdown.index("## 今日结论卡") < markdown.index("## 模块评分")
    assert "### 什么情况会改变判断" in markdown
    assert "分模块变化：" in markdown
    assert "趋势（trend）" in markdown
    assert "最终动作约束" in markdown
    assert "thesis 承压" in markdown
    assert "- AI 产业链评分：" in markdown
    assert "- 判断置信度：" in markdown
    assert "- 置信度调整后模型仓位" in markdown
    assert "| 模块 | 分数 | 权重 | 来源 | 覆盖率 | 置信度 | 说明 |" in markdown
    assert "评分模型仓位（score_model）" in markdown
    assert "交易 thesis" in markdown
    assert "基本面（fundamentals）" in markdown
    assert "基本面硬数据占位" in markdown


def test_risk_event_openai_daily_section_includes_transport_client() -> None:
    section = cli_module._risk_event_openai_precheck_daily_section(
        official_policy_fetch_report=SimpleNamespace(
            status="PASS",
            payload_count=8,
            candidate_count=20,
        ),
        risk_event_prereview_report=SimpleNamespace(
            status="PASS_WITH_WARNINGS",
            row_count=20,
            record_count=7,
            high_level_candidate_count=6,
            active_candidate_count=1,
            openai_cache_hit_count=2,
            openai_cache_miss_count=18,
            openai_cache_expired_count=0,
            openai_cache_disabled_count=0,
        ),
        official_policy_report_output=Path("outputs/reports/official_policy_sources.md"),
        risk_event_openai_precheck_report_output=Path(
            "outputs/reports/risk_event_prereview_openai.md"
        ),
        risk_event_prereview_queue_path=Path("data/processed/risk_event_prereview_queue.json"),
        llm_formal_report=SimpleNamespace(
            status="PASS_WITH_WARNINGS",
            occurrence_count=3,
            active_occurrence_count=1,
            watch_occurrence_count=2,
            attestation=object(),
        ),
        risk_event_llm_formal_report_output=Path(
            "outputs/reports/risk_event_llm_formal_assessment.md"
        ),
        llm_formal_enabled=True,
        model="gpt-5.5",
        reasoning_effort="high",
        timeout_seconds=120.0,
        http_client="requests",
        cache_dir=Path("data/processed/agent_request_cache"),
        cache_ttl_hours=24.0,
        max_candidates=20,
    )

    assert "- HTTP client：requests" in section
    assert "- OpenAI 请求缓存 TTL：24 小时" in section
    assert "HIT=2 / MISS=18" in section
    assert "- 待人工复核队列记录数：7" in section
    assert "- LLM formal 自动写入：是" in section
    assert "- LLM formal occurrence 数：3" in section
    assert "LLM formal trusted by owner" in section
    assert "`execution_policy.manual_review_gate_ids`" in section
    assert "不会单独把执行动作改成 `wait_manual_review`" in section


def test_belief_state_is_read_only_and_keeps_position_unchanged(
    tmp_path: Path,
) -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        risk_event_occurrence_review_report=_risk_event_occurrence_review_report(),
    )
    before = (
        report.recommendation.risk_asset_ai_band.min_position,
        report.recommendation.risk_asset_ai_band.max_position,
        tuple(gate.max_position for gate in report.recommendation.position_gates),
    )

    belief_state = build_belief_state(
        report=report,
        trace_bundle_path=tmp_path / "daily_trace.json",
        decision_snapshot_path=tmp_path / "decision_snapshot.json",
        market_regime=None,
        config_paths={"scoring_rules": tmp_path / "scoring_rules.yaml"},
    )
    summary = render_belief_state_summary(
        belief_state,
        tmp_path / "belief_state.json",
    )

    after = (
        report.recommendation.risk_asset_ai_band.min_position,
        report.recommendation.risk_asset_ai_band.max_position,
        tuple(gate.max_position for gate in report.recommendation.position_gates),
    )
    assert after == before
    assert belief_state["read_only"] is True
    assert belief_state["production_effect"] == "none"
    assert belief_state["position_boundary"]["final_risk_asset_ai_band"][
        "max_position"
    ] == report.recommendation.risk_asset_ai_band.max_position
    assert belief_state["references"]["overall_claim_id"] == (
        "daily_score:2026-04-30:overall_position"
    )
    assert "只读解释层" in summary
    assert "不改变正式评分" in summary
    assert "总风险资产预算" in summary


def test_render_daily_score_report_includes_sec_feature_gate(tmp_path: Path) -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        fundamental_feature_report=_fundamental_feature_report(),
    )

    markdown = render_daily_score_report(
        report,
        data_quality_report_path=tmp_path / "quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features.csv",
        scores_path=tmp_path / "scores.csv",
        sec_metrics_validation_report_path=tmp_path / "sec_validation.md",
        sec_fundamental_feature_report_path=tmp_path / "sec_features.md",
        sec_fundamental_features_path=tmp_path / "sec_features.csv",
    )

    assert "- SEC 指标 CSV 校验状态：PASS" in markdown
    assert "- SEC 基本面特征状态：PASS" in markdown
    assert "sec_features.csv" in markdown


def test_render_daily_score_report_includes_valuation_history_coverage(
    tmp_path: Path,
) -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        valuation_review_report=_valuation_review_report(),
    )

    markdown = render_daily_score_report(
        report,
        data_quality_report_path=tmp_path / "quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features.csv",
        scores_path=tmp_path / "scores.csv",
    )

    assert "- 当前估值复核快照数：1" in markdown
    assert "- 估值历史指标覆盖：valuation_percentile 1/1；eps_revision_90d_pct 0/1" in markdown


def test_score_daily_cli_writes_report_and_scores(tmp_path: Path) -> None:
    universe = load_universe()
    tickers = configured_price_tickers(universe)
    rate_series = configured_rate_series(universe)
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    features_path = tmp_path / "features_daily.csv"
    scores_path = tmp_path / "scores_daily.csv"
    daily_report_path = tmp_path / "daily_score.md"
    feature_report_path = tmp_path / "feature_summary.md"
    feature_availability_report_path = tmp_path / "feature_availability.md"
    quality_report_path = tmp_path / "quality.md"
    execution_policy_report_path = tmp_path / "execution_policy.md"
    portfolio_positions_path = tmp_path / "portfolio_positions.csv"
    portfolio_exposure_report_path = tmp_path / "portfolio_exposure.md"
    sec_companies_path = tmp_path / "sec_companies.yaml"
    sec_metrics_path = tmp_path / "fundamental_metrics.yaml"
    fundamental_feature_config_path = tmp_path / "fundamental_features.yaml"
    sec_fundamentals_path = tmp_path / "sec_fundamentals.csv"
    sec_features_path = tmp_path / "sec_fundamental_features.csv"
    sec_feature_report_path = tmp_path / "sec_fundamental_features.md"
    sec_validation_report_path = tmp_path / "sec_fundamentals_validation.md"
    valuation_path = tmp_path / "valuation_snapshots"
    risk_event_occurrences_path = tmp_path / "risk_event_occurrences"
    decision_snapshot_path = tmp_path / "decision_snapshot.json"
    prediction_ledger_path = tmp_path / "prediction_ledger.csv"
    belief_state_path = tmp_path / "belief_state.json"
    belief_state_history_path = tmp_path / "belief_state_history.csv"
    _sample_prices(tickers, periods=260).to_csv(prices_path, index=False)
    _sample_rates(rate_series, periods=260).to_csv(rates_path, index=False)
    _write_sec_companies_config(sec_companies_path)
    _write_sec_metrics_config(sec_metrics_path)
    _write_sec_features_config(fundamental_feature_config_path)
    _write_sec_metrics_csv(sec_fundamentals_path)

    result = CliRunner().invoke(
        app,
        [
            "score-daily",
            "--skip-risk-event-openai-precheck",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2026-04-30",
            "--run-id",
            "daily_ops_run:2026-04-30:test",
            "--features-path",
            str(features_path),
            "--scores-path",
            str(scores_path),
            "--report-path",
            str(daily_report_path),
            "--feature-report-path",
            str(feature_report_path),
            "--feature-availability-report-path",
            str(feature_availability_report_path),
            "--quality-report-path",
            str(quality_report_path),
            "--execution-policy-report-path",
            str(execution_policy_report_path),
            "--portfolio-positions-path",
            str(portfolio_positions_path),
            "--portfolio-exposure-report-path",
            str(portfolio_exposure_report_path),
            "--sec-companies-path",
            str(sec_companies_path),
            "--sec-metrics-path",
            str(sec_metrics_path),
            "--fundamental-feature-config-path",
            str(fundamental_feature_config_path),
            "--sec-fundamentals-path",
            str(sec_fundamentals_path),
            "--sec-fundamental-features-path",
            str(sec_features_path),
            "--sec-fundamental-feature-report-path",
            str(sec_feature_report_path),
            "--sec-metrics-validation-report-path",
            str(sec_validation_report_path),
            "--valuation-path",
            str(valuation_path),
            "--risk-event-occurrences-path",
            str(risk_event_occurrences_path),
            "--decision-snapshot-path",
            str(decision_snapshot_path),
            "--prediction-ledger-path",
            str(prediction_ledger_path),
            "--belief-state-path",
            str(belief_state_path),
            "--belief-state-history-path",
            str(belief_state_history_path),
        ],
    )

    assert result.exit_code == 0
    assert daily_report_path.exists()
    assert scores_path.exists()
    assert execution_policy_report_path.exists()
    assert portfolio_exposure_report_path.exists()
    assert feature_availability_report_path.exists()
    assert prediction_ledger_path.exists()
    trace_path = tmp_path / "evidence" / "daily_score_trace.json"
    assert trace_path.exists()
    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    snapshot = json.loads(decision_snapshot_path.read_text(encoding="utf-8"))
    belief_state = json.loads(belief_state_path.read_text(encoding="utf-8"))
    belief_history = pd.read_csv(belief_state_history_path)
    claim_ids = {claim["claim_id"] for claim in trace["claims"]}
    dataset_ids = {dataset["dataset_id"] for dataset in trace["dataset_refs"]}
    assert "daily_score:2026-04-30:overall_position" in claim_ids
    assert "daily_score:2026-04-30:score_architecture" in claim_ids
    assert "daily_score:2026-04-30:belief_state" in claim_ids
    assert "daily_score:2026-04-30:focus_stock_trends" in claim_ids
    assert trace["run_manifest"]["run_id"] == "daily_ops_run:2026-04-30:test"
    assert "dataset:belief_state:2026-04-30" in dataset_ids
    assert "dataset:feature_availability:2026-04-30" in dataset_ids
    feature_availability_params = trace["run_manifest"]["parameters"][
        "feature_availability"
    ]
    assert feature_availability_params["status"] == "PASS"
    assert "sec_fundamental_features" in feature_availability_params["required_sources"]
    assert snapshot["snapshot_id"] == "decision_snapshot:2026-04-30"
    assert snapshot["trace"]["trace_bundle_path"] == str(trace_path)
    assert snapshot["belief_state_ref"]["path"] == str(belief_state_path)
    assert snapshot["belief_state_ref"]["production_effect"] == "none"
    trace_rule_versions = trace["run_manifest"]["parameters"]["rule_versions"]
    snapshot_rule_versions = snapshot["rule_versions"]
    assert trace_rule_versions["applies_to"] == "score-daily"
    assert trace_rule_versions["production_rule_count"] >= 1
    assert snapshot_rule_versions["applies_to"] == "score-daily"
    rule_ids = {rule["rule_id"] for rule in snapshot_rule_versions["rules"]}
    assert "scoring.weighted_score.v1" in rule_ids
    assert belief_state["belief_state_id"] == "belief_state:2026-04-30"
    assert belief_state["production_effect"] == "none"
    assert set(belief_history["belief_state_id"]) == {"belief_state:2026-04-30"}
    assert belief_history.iloc[0]["production_effect"] == "none"
    assert snapshot["scores"]["confidence_level"] in {"high", "medium", "low"}
    assert snapshot["score_architecture"]["production_effect"] == "none"
    assert snapshot["score_architecture"]["base_signal_components"] == [
        "trend",
        "fundamentals",
    ]
    assert snapshot["positions"]["position_gates"]
    assert features_path.exists()
    assert sec_features_path.exists()
    assert sec_feature_report_path.exists()
    assert sec_validation_report_path.exists()
    daily_text = daily_report_path.read_text(encoding="utf-8")
    assert "人工复核摘要" in daily_text
    assert "SEC 基本面特征状态：PASS" in daily_text
    assert "风险事件发生记录状态：" in daily_text
    assert "## 变化原因树" in daily_text
    assert "## 认知状态" in daily_text
    assert "## 执行建议" in daily_text
    assert "## 组合暴露" in daily_text
    assert "NOT_CONNECTED" in daily_text
    assert "风险预算" in daily_text
    assert "## 今日结论卡" in daily_text
    assert "| Data Gate | PASS_WITH_WARNINGS；存在质量警告" in daily_text
    assert "| Run ID / Trace | daily_ops_run:2026-04-30:test；trace=" in daily_text
    assert "### Main Invalidator" in daily_text
    assert "### Next Checks" in daily_text
    assert "## Base Signal / Risk Caps" in daily_text
    assert "## 结论使用等级" in daily_text
    assert "适用范围：趋势判断/投研辅助，不触发交易（`trend_judgment`）" in daily_text
    assert "## 关注股票趋势分析" in daily_text
    assert "MSFT" in daily_text
    assert "## 产业链节点热度与健康度" in daily_text
    assert "健康度边界" in daily_text
    assert "执行动作 | 观察，不形成交易结论（`observe_only`）" in daily_text
    assert "- 生产影响：none" in daily_text
    assert "执行政策校验：PASS" in daily_text
    assert "## 可追溯引用" in daily_text
    assert "## PIT 特征可见时间" in daily_text
    assert "daily_score:2026-04-30:overall_position" in daily_text
    feature_text = feature_report_path.read_text(encoding="utf-8")
    assert "## PIT 特征可见时间" in feature_text
    feature_availability_text = feature_availability_report_path.read_text(
        encoding="utf-8"
    )
    assert "# PIT 特征可见时间报告" in feature_availability_text
    prediction_frame = pd.read_csv(prediction_ledger_path)
    assert set(prediction_frame["candidate_id"]) == {"production"}
    assert set(prediction_frame["production_effect"]) == {"production"}
    assert set(prediction_frame["outcome_status"]) == {"PENDING"}
    assert "每日评分状态：" in result.output
    assert "执行建议：" in result.output
    assert "产业链节点热度与健康度：" in result.output
    assert "关注股票趋势分析：" in result.output
    assert "组合暴露：" in result.output
    assert "Prediction" in result.output
    assert "belief_state.json" in result.output
    lookup_result = CliRunner().invoke(
        app,
        [
            "trace",
            "lookup",
            "--bundle-path",
            str(trace_path),
            "--id",
            "daily_score:2026-04-30:overall_position",
        ],
    )
    assert lookup_result.exit_code == 0
    assert "最终 AI 仓位" in lookup_result.output


def test_score_daily_tsm_ir_merge_helper_replaces_tsm_quarterly_rows(
    tmp_path: Path,
) -> None:
    sec_companies_path = tmp_path / "sec_companies.yaml"
    sec_companies_path.write_text(
        """
companies:
  - ticker: TSM
    cik: "0001046179"
    company_name: Taiwan Semiconductor Manufacturing Company Limited
    sec_metric_periods:
      - annual
      - quarterly
    expected_taxonomies:
      - ifrs-full
""",
        encoding="utf-8",
    )
    sec_companies = load_sec_companies(sec_companies_path)
    sec_path = tmp_path / "sec_fundamentals.csv"
    tsm_path = tmp_path / "tsm_ir_quarterly_metrics.csv"
    source_path = tmp_path / "tsm_management_report.txt"

    write_sec_fundamental_metric_rows_csv(
        (
            SecFundamentalMetricRow(
                as_of=date(2026, 5, 10),
                ticker="TSM",
                cik="0001046179",
                company_name="Taiwan Semiconductor Manufacturing Company Limited",
                metric_id="revenue",
                metric_name="Revenue",
                period_type="quarterly",
                fiscal_year=2026,
                fiscal_period="Q1",
                end_date=date(2026, 3, 31),
                filed_date=date(2026, 4, 16),
                form="6-K",
                taxonomy="ifrs-full",
                concept="Revenue",
                unit="TWD_billions",
                value=1.0,
                accession_number="old-tsm-quarter",
                source_path=source_path,
            ),
        ),
        sec_path,
    )
    tsm_ir_quarterly_metric_rows_to_frame(
        (
            TsmIrQuarterlyMetricRow(
                as_of=date(2026, 5, 10),
                ticker="TSM",
                fiscal_year=2026,
                fiscal_period="Q1",
                end_date=date(2026, 3, 31),
                filed_date=date(2026, 4, 16),
                captured_at=datetime(2026, 5, 10, 12, tzinfo=UTC),
                metric_id="revenue",
                metric_name="Revenue",
                period_type="quarterly",
                unit="TWD_billions",
                value=839.25,
                source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
                source_path=source_path,
                source_id="tsm_ir_2026_q1",
                checksum_sha256="a" * 64,
            ),
        )
    ).to_csv(tsm_path, index=False)

    cli_module._merge_tsm_ir_for_daily_score(
        sec_fundamentals_path=sec_path,
        tsm_ir_path=tsm_path,
        sec_companies=sec_companies,
        as_of=date(2026, 5, 10),
    )

    rows = load_sec_fundamental_metric_rows_csv(sec_path)
    assert len(rows) == 1
    assert rows[0].ticker == "TSM"
    assert rows[0].form == "TSM-IR"
    assert rows[0].value == 839.25
    assert rows[0].accession_number.endswith(":FY2026Q1")


def _risk_event_occurrence_review_report(
    *,
    status: str = "active",
    evidence_grade: str = "A",
    action_class: str = "position_gate_eligible",
) -> RiskEventOccurrenceReviewReport:
    as_of = date(2026, 4, 30)
    validation_report = RiskEventOccurrenceValidationReport(
        as_of=as_of,
        input_path=Path("risk_event_occurrences"),
        config=load_risk_events(),
        occurrences=(
            LoadedRiskEventOccurrence(
                occurrence=RiskEventOccurrence(
                    occurrence_id="taiwan_geopolitical_escalation_2026_04_30",
                    event_id="taiwan_geopolitical_escalation",
                    status=status,
                    triggered_at=as_of,
                    last_confirmed_at=as_of,
                    evidence_grade=evidence_grade,
                    severity="high",
                    probability="confirmed",
                    scope="ai_bucket",
                    time_sensitivity="high",
                    reversibility="partly_reversible",
                    action_class=action_class,
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


def _empty_risk_event_occurrence_review_report_with_attestation() -> (
    RiskEventOccurrenceReviewReport
):
    as_of = date(2026, 4, 30)
    validation_report = RiskEventOccurrenceValidationReport(
        as_of=as_of,
        input_path=Path("risk_event_occurrences"),
        config=load_risk_events(),
        occurrences=(),
        review_attestations=(
            LoadedRiskEventReviewAttestation(
                attestation=RiskEventReviewAttestation(
                    attestation_id="risk_event_review_attestation_2026_04_30",
                    review_date=as_of,
                    coverage_start=as_of,
                    coverage_end=as_of,
                    reviewer="policy_owner",
                    reviewed_at=as_of,
                    review_decision="confirmed_no_unrecorded_material_events",
                    rationale="人工复核官方来源和预审队列，未发现未记录重大风险事件。",
                    next_review_due=date(2026, 5, 1),
                    review_scope=[
                        "policy_event_occurrences",
                        "geopolitical_event_occurrences",
                    ],
                    checked_sources=[
                        RiskEventReviewAttestationSource(
                            source_name="manual_daily_risk_review",
                            source_type="manual_input",
                            captured_at=as_of,
                        )
                    ],
                ),
                path=Path("risk_event_occurrences/review_attestation.yaml"),
            ),
        ),
    )
    return build_risk_event_occurrence_review_report(validation_report)


def _empty_risk_event_occurrence_review_report_with_llm_formal_attestation() -> (
    RiskEventOccurrenceReviewReport
):
    as_of = date(2026, 4, 30)
    validation_report = RiskEventOccurrenceValidationReport(
        as_of=as_of,
        input_path=Path("risk_event_occurrences"),
        config=load_risk_events(),
        occurrences=(),
        review_attestations=(
            LoadedRiskEventReviewAttestation(
                attestation=RiskEventReviewAttestation(
                    attestation_id="llm_formal_risk_event_assessment_2026_04_30",
                    review_date=as_of,
                    coverage_start=as_of,
                    coverage_end=as_of,
                    reviewer="llm_formal_assessment",
                    reviewed_at=as_of,
                    review_decision="confirmed_no_unrecorded_material_events",
                    rationale="LLM formal assessment 未发现队列内 active 风险事件。",
                    next_review_due=date(2026, 5, 1),
                    review_scope=[
                        "llm_triaged_high_priority_official_candidates",
                        "policy_event_occurrences",
                        "geopolitical_event_occurrences",
                    ],
                    checked_sources=[
                        RiskEventReviewAttestationSource(
                            source_name="OpenAI risk_event_prereview_queue",
                            source_type="llm_extracted",
                            captured_at=as_of,
                        ),
                        RiskEventReviewAttestationSource(
                            source_name="Congress.gov API",
                            source_type="primary_source",
                            source_url="https://api.congress.gov/v3/bill",
                            captured_at=as_of,
                        ),
                    ],
                ),
                path=Path("risk_event_occurrences/llm_formal_attestation.yaml"),
            ),
        ),
    )
    return build_risk_event_occurrence_review_report(validation_report)


def _fundamental_feature_report() -> SecFundamentalFeaturesReport:
    as_of = date(2026, 4, 30)
    rows = (
        _fundamental_feature(as_of, "NVDA", "gross_margin", "quarterly", 0.72),
        _fundamental_feature(as_of, "MSFT", "gross_margin", "quarterly", 0.69),
        _fundamental_feature(as_of, "NVDA", "operating_margin", "quarterly", 0.43),
        _fundamental_feature(as_of, "MSFT", "operating_margin", "quarterly", 0.42),
        _fundamental_feature(as_of, "NVDA", "net_margin", "quarterly", 0.35),
        _fundamental_feature(as_of, "MSFT", "net_margin", "quarterly", 0.34),
        _fundamental_feature(
            as_of,
            "NVDA",
            "research_and_development_intensity",
            "quarterly",
            0.12,
        ),
        _fundamental_feature(
            as_of,
            "MSFT",
            "research_and_development_intensity",
            "quarterly",
            0.13,
        ),
        _fundamental_feature(as_of, "NVDA", "capex_intensity", "annual", 0.14),
        _fundamental_feature(as_of, "MSFT", "capex_intensity", "annual", 0.16),
    )
    return SecFundamentalFeaturesReport(
        as_of=as_of,
        input_path=Path("sec_fundamentals.csv"),
        validation_report=SecFundamentalMetricsCsvValidationReport(
            as_of=as_of,
            input_path=Path("sec_fundamentals.csv"),
            row_count=20,
            as_of_row_count=20,
            expected_observation_count=20,
            observed_observation_count=20,
        ),
        rows=rows,
    )


def _valuation_review_report(
    *,
    valuation_percentile: float = 82.0,
    overall_assessment: str = "expensive",
) -> ValuationReviewReport:
    as_of = date(2026, 4, 30)
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
                    valuation_percentile=valuation_percentile,
                    overall_assessment=overall_assessment,
                ),
                path=Path("nvda_valuation.yaml"),
            ),
        ),
    )
    return build_valuation_review_report(validation_report)


def _fundamental_feature(
    as_of: date,
    ticker: str,
    feature_id: str,
    period_type: str,
    value: float,
) -> SecFundamentalFeatureRow:
    return SecFundamentalFeatureRow(
        as_of=as_of,
        ticker=ticker,
        period_type=cast(PeriodType, period_type),
        fiscal_year=2026,
        fiscal_period="Q1" if period_type == "quarterly" else "FY",
        end_date=as_of,
        filed_date=as_of,
        feature_id=feature_id,
        feature_name=feature_id.replace("_", " ").title(),
        value=value,
        unit="ratio",
        numerator_metric_id="numerator",
        denominator_metric_id="revenue",
        numerator_value=value * 1000,
        denominator_value=1000,
        source_metric_accessions="0000000000-26-000001",
        source_path=Path("sec_fundamentals.csv"),
    )


def _feature_set() -> MarketFeatureSet:
    as_of = date(2026, 4, 30)
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
        _feature(as_of, "macro_liquidity", "DTWEXBGS", "return_20d", -0.02),
        _feature(as_of, "risk_sentiment", "^VIX", "vix_current", 17.0),
        _feature(as_of, "risk_sentiment", "^VIX", "vix_percentile_252", 0.35),
        _feature(as_of, "trend", "^VIX", "return_5d", -0.05),
    ]
    return MarketFeatureSet(as_of=as_of, rows=tuple(rows), warnings=())


def _feature_set_with_vix(
    *,
    vix_current: float,
    vix_percentile: float,
) -> MarketFeatureSet:
    base = _feature_set()
    rows = []
    for row in base.rows:
        if row.subject == "^VIX" and row.feature == "vix_current":
            rows.append(replace(row, value=vix_current))
        elif row.subject == "^VIX" and row.feature == "vix_percentile_252":
            rows.append(replace(row, value=vix_percentile))
        else:
            rows.append(row)
    return MarketFeatureSet(as_of=base.as_of, rows=tuple(rows), warnings=base.warnings)


def _feature_set_with_macro_stress(
    *,
    dgs10_rate_change_20d: float,
    dollar_return_20d: float,
) -> MarketFeatureSet:
    base = _feature_set()
    rows = []
    for row in base.rows:
        if row.subject == "DGS10" and row.feature == "rate_change_20d":
            rows.append(replace(row, value=dgs10_rate_change_20d))
        elif row.subject == "DTWEXBGS" and row.feature == "return_20d":
            rows.append(replace(row, value=dollar_return_20d))
        else:
            rows.append(row)
    return MarketFeatureSet(as_of=base.as_of, rows=tuple(rows), warnings=base.warnings)


def _concentrated_portfolio_exposure_report(tmp_path: Path) -> PortfolioExposureReport:
    input_path = tmp_path / "positions.csv"
    input_path.write_text(
        "\n".join(
            [
                (
                    "as_of,ticker,instrument_type,quantity,market_value,currency,"
                    "ai_exposure_pct,region,customer_chain,factor_tags,"
                    "correlation_cluster,etf_beta_to_ai_proxy,notes"
                ),
                (
                    "2026-04-30,NVDA,single_stock,10,10000,USD,1.0,US,"
                    "hyperscaler_capex,growth;semiconductor,ai_semis,,test"
                ),
                "2026-04-30,USD_CASH,cash,1,5000,USD,0,US,cash,cash,cash,,cash",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return build_portfolio_exposure_report(
        input_path=input_path,
        as_of=date(2026, 4, 30),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
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


def _component(report, name: str):  # type: ignore[no-untyped-def]
    for component in report.components:
        if component.name == name:
            return component
    raise AssertionError(f"component not found: {name}")


def _position_gate(report, gate_id: str):  # type: ignore[no-untyped-def]
    for gate in report.recommendation.position_gates:
        if gate.gate_id == gate_id:
            return gate
    raise AssertionError(f"position gate not found: {gate_id}")


def _quality_report() -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-05-01T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 4, 30),
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=1),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1),
        expected_price_tickers=("SPY",),
        expected_rate_series=("DGS10",),
        issues=(),
    )


def _sample_prices(tickers: list[str], periods: int) -> pd.DataFrame:
    dates = pd.date_range(end="2026-04-30", periods=periods, freq="D")
    rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        base = 100.0 + ticker_index * 10.0
        daily_step = 1.0 + ticker_index * 0.05
        for day_index, row_date in enumerate(dates):
            close = base + day_index * daily_step
            rows.append(
                {
                    "date": row_date.date().isoformat(),
                    "ticker": ticker,
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "adj_close": close,
                    "volume": 1_000_000 + ticker_index,
                }
            )
    return pd.DataFrame(rows)


def _sample_rates(series_ids: list[str], periods: int) -> pd.DataFrame:
    dates = pd.date_range(end="2026-04-30", periods=periods, freq="D")
    rows: list[dict[str, object]] = []
    for series_index, series_id in enumerate(series_ids):
        base = 4.0 + series_index * 0.2
        for day_index, row_date in enumerate(dates):
            rows.append(
                {
                    "date": row_date.date().isoformat(),
                    "series": series_id,
                    "value": base - day_index * 0.001,
                }
            )
    return pd.DataFrame(rows)


def _write_sec_companies_config(output_path: Path) -> None:
    output_path.write_text(
        """
companies:
  - ticker: NVDA
    cik: "0001045810"
    company_name: NVIDIA Corporation
    sec_metric_periods:
      - annual
    expected_taxonomies:
      - us-gaap
""",
        encoding="utf-8",
    )


def _write_sec_metrics_config(output_path: Path) -> None:
    output_path.write_text(
        """
metrics:
  - metric_id: revenue
    name: Revenue
    description: SEC companyfacts 披露的总收入。
    preferred_periods:
      - annual
    concepts:
      - taxonomy: us-gaap
        concept: Revenues
        unit: USD
  - metric_id: gross_profit
    name: Gross Profit
    description: 已披露时使用收入扣除营业成本后的毛利。
    preferred_periods:
      - annual
    concepts:
      - taxonomy: us-gaap
        concept: GrossProfit
        unit: USD
""",
        encoding="utf-8",
    )


def _write_sec_features_config(output_path: Path) -> None:
    output_path.write_text(
        """
features:
  - feature_id: gross_margin
    name: Gross Margin
    description: 毛利除以收入。
    numerator_metric_id: gross_profit
    denominator_metric_id: revenue
    preferred_periods:
      - annual
""",
        encoding="utf-8",
    )


def _write_sec_metrics_csv(output_path: Path) -> None:
    pd.DataFrame(
        [
            _sec_metric_record("revenue", "Revenue", 1000),
            _sec_metric_record("gross_profit", "Gross Profit", 650),
        ],
        columns=list(SEC_FUNDAMENTAL_METRIC_COLUMNS),
    ).to_csv(output_path, index=False)


def _sec_metric_record(metric_id: str, metric_name: str, value: float) -> dict[str, object]:
    return {
        "as_of": "2026-04-30",
        "ticker": "NVDA",
        "cik": "0001045810",
        "company_name": "NVIDIA Corporation",
        "metric_id": metric_id,
        "metric_name": metric_name,
        "period_type": "annual",
        "fiscal_year": 2026,
        "fiscal_period": "FY",
        "end_date": "2026-01-31",
        "filed_date": "2026-02-27",
        "form": "10-K",
        "taxonomy": "us-gaap",
        "concept": "Revenues" if metric_id == "revenue" else "GrossProfit",
        "unit": "USD",
        "value": value,
        "accession_number": "0001045810-26-000001",
        "source_path": "nvda_companyfacts.json",
    }
