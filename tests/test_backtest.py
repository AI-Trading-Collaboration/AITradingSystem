from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import cast

import pandas as pd
from pytest import MonkeyPatch
from typer.testing import CliRunner

import ai_trading_system.backtest.daily as backtest_daily_module
from ai_trading_system.backtest.audit import (
    build_backtest_audit_report,
    render_backtest_audit_report,
)
from ai_trading_system.backtest.daily import (
    backtest_input_coverage_records,
    render_backtest_report,
    run_daily_score_backtest,
    write_backtest_daily_csv,
    write_backtest_input_coverage_csv,
)
from ai_trading_system.cli import app
from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_features,
    load_portfolio,
    load_risk_events,
    load_scoring_rules,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityIssue,
    DataQualityReport,
    Severity,
)
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeatureRow,
    SecFundamentalFeaturesReport,
)
from ai_trading_system.fundamentals.sec_metrics import (
    PeriodType,
    SecFundamentalMetricsCsvValidationReport,
    SecMetricIssue,
    SecMetricIssueSeverity,
)
from ai_trading_system.risk_events import (
    LoadedRiskEventOccurrence,
    RiskEventEvidenceSource,
    RiskEventIssue,
    RiskEventIssueSeverity,
    RiskEventOccurrence,
    RiskEventOccurrenceReviewReport,
    RiskEventOccurrenceSourceType,
    RiskEventOccurrenceValidationReport,
    build_risk_event_occurrence_review_report,
)
from ai_trading_system.valuation import (
    LoadedValuationSnapshot,
    SnapshotMetric,
    ValuationIssue,
    ValuationIssueSeverity,
    ValuationReviewReport,
    ValuationSnapshot,
    ValuationSourceType,
    ValuationValidationReport,
    build_valuation_review_report,
)
from ai_trading_system.watchlist_lifecycle import (
    WatchlistLifecycleConfig,
    WatchlistLifecycleEntry,
)


def test_run_daily_score_backtest_uses_next_day_returns() -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)

    result = run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY", "QQQ"),
        cost_bps=5.0,
    )

    assert result.rows
    assert result.rows[0].return_date > result.rows[0].signal_date
    assert result.strategy_metrics.total_return > 0
    assert set(result.benchmark_metrics) == {"SPY", "QQQ"}


def test_run_daily_score_backtest_deducts_configured_slippage() -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)

    base_result = run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        cost_bps=5.0,
    )
    slippage_result = run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        cost_bps=5.0,
        slippage_bps=10.0,
    )

    assert slippage_result.slippage_bps == 10.0
    assert slippage_result.rows[0].slippage_cost > 0
    assert slippage_result.rows[0].transaction_cost > base_result.rows[0].transaction_cost
    assert (
        slippage_result.strategy_metrics.total_return
        < base_result.strategy_metrics.total_return
    )


def test_run_daily_score_backtest_uses_point_in_time_sec_features() -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)
    signal_dates = pd.date_range("2026-04-01", "2026-04-29", freq="D")
    sec_reports = {
        item.date(): _fundamental_feature_report(item.date()) for item in signal_dates
    }

    result = run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        fundamental_feature_reports=sec_reports,
    )

    assert result.fundamental_feature_report_count == len(result.rows)
    assert result.rows[0].component_scores["fundamentals"] > 50
    assert result.rows[0].component_source_types["fundamentals"] == "hard_data"
    assert result.rows[0].component_coverages["fundamentals"] == 1.0


def test_run_daily_score_backtest_uses_point_in_time_valuation_and_risk_events() -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)
    signal_dates = pd.date_range("2026-04-01", "2026-04-29", freq="D")
    valuation_reports = {
        item.date(): _valuation_review_report(item.date()) for item in signal_dates
    }
    risk_event_reports = {
        item.date(): _risk_event_occurrence_review_report(item.date())
        for item in signal_dates
    }

    result = run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        valuation_review_reports=valuation_reports,
        risk_event_occurrence_review_reports=risk_event_reports,
    )

    assert result.valuation_review_report_count == len(result.rows)
    assert result.risk_event_occurrence_review_report_count == len(result.rows)
    assert result.rows[0].component_source_types["valuation"] == "manual_input"
    assert result.rows[0].component_source_types["policy_geopolitics"] == "manual_input"
    assert result.rows[0].component_coverages["valuation"] == 1.0
    assert result.rows[0].component_coverages["policy_geopolitics"] == 1.0
    assert result.rows[0].position_gate_triggers["risk_events"]
    assert result.rows[0].model_target_exposure > result.rows[0].gated_target_exposure


def test_run_daily_score_backtest_filters_watchlist_by_lifecycle(
    monkeypatch: MonkeyPatch,
) -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)
    captured_watchlists: list[tuple[date, tuple[str, ...]]] = []
    original_build_market_features = backtest_daily_module.build_market_features

    def wrapped_build_market_features(**kwargs):  # type: ignore[no-untyped-def]
        captured_watchlists.append(
            (kwargs["as_of"], tuple(kwargs["core_watchlist"]))
        )
        return original_build_market_features(**kwargs)

    monkeypatch.setattr(
        backtest_daily_module,
        "build_market_features",
        wrapped_build_market_features,
    )
    lifecycle = WatchlistLifecycleConfig(
        entries=[
            WatchlistLifecycleEntry(
                ticker="NVDA",
                added_at=date(2026, 4, 2),
                reason="测试在回测第二个 signal_date 才加入观察池。",
                active_from=date(2026, 4, 2),
                competence_status="in_competence",
                node_mapping_valid_from=date(2026, 4, 2),
                thesis_required_from=date(2026, 4, 2),
                source="unit_test",
            )
        ]
    )

    run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 4),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        watchlist_lifecycle=lifecycle,
    )

    captured_by_date = dict(captured_watchlists)
    assert captured_by_date[date(2026, 4, 1)] == ()
    assert captured_by_date[date(2026, 4, 2)] == ("NVDA",)


def test_render_and_write_backtest_outputs(tmp_path: Path) -> None:
    universe = load_universe()
    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
    )

    daily_path = write_backtest_daily_csv(result, tmp_path / "daily.csv")
    input_coverage_path = write_backtest_input_coverage_csv(
        result,
        tmp_path / "input_coverage.csv",
    )
    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=daily_path,
        input_coverage_output_path=input_coverage_path,
        audit_report_path=tmp_path / "audit.md",
    )

    assert daily_path.exists()
    assert input_coverage_path.exists()
    assert "# 历史回测报告" in markdown
    assert "基准（SPY 买入持有）" in markdown
    assert "历史输入覆盖诊断" in markdown
    assert "输入审计报告" in markdown
    assert "线性滑点/盘口冲击估算：0.0 bps" in markdown
    assert "## 执行成本摘要" in markdown
    assert "## 仓位闸门摘要" in markdown
    assert "## 判断置信度摘要" in markdown
    assert "| 单边交易成本扣减 |" in markdown
    assert "| 线性滑点扣减 |" in markdown
    assert "fundamentals_source_type" in daily_path.read_text(encoding="utf-8")
    daily_text = daily_path.read_text(encoding="utf-8")
    assert "confidence_score" in daily_text
    assert "confidence_level" in daily_text
    assert "commission_cost" in daily_text
    assert "slippage_cost" in daily_text
    assert "risk_events_gate_triggered" in daily_text
    assert "component_coverage" in input_coverage_path.read_text(encoding="utf-8")


def test_backtest_audit_report_flags_input_gaps(tmp_path: Path) -> None:
    universe = load_universe()
    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
    )

    audit_report = build_backtest_audit_report(
        result=result,
        data_quality_report_path=tmp_path / "quality.md",
        backtest_report_path=tmp_path / "backtest.md",
        daily_output_path=tmp_path / "daily.csv",
        input_coverage_output_path=tmp_path / "input_coverage.csv",
        minimum_component_coverage=0.9,
    )
    markdown = render_backtest_audit_report(audit_report)

    assert audit_report.status == "PASS_WITH_WARNINGS"
    assert "# 回测输入审计报告" in markdown
    assert "## Point-in-Time 输入" in markdown
    assert "sec_point_in_time_slice_incomplete" in markdown
    assert "valuation_point_in_time_slice_incomplete" in markdown
    assert "risk_event_point_in_time_slice_incomplete" in markdown
    assert "## 执行假设" in markdown


def test_backtest_input_coverage_csv_includes_audit_records(tmp_path: Path) -> None:
    universe = load_universe()
    signal_dates = [date(2026, 4, day) for day in range(1, 3)]
    fundamental_reports = {
        signal_date: _fundamental_feature_report(signal_date)
        for signal_date in signal_dates
    }
    valuation_reports = {
        signal_date: _valuation_review_report(
            signal_date,
            source_type="paid_vendor",
            source_url="https://vendor.example/nvda-valuation",
        )
        for signal_date in signal_dates
    }
    risk_reports = {
        signal_date: _risk_event_occurrence_review_report(
            signal_date,
            source_types=("primary_source",),
            source_url="https://policy.example/release",
        )
        for signal_date in signal_dates
    }

    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 3),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        fundamental_feature_reports=fundamental_reports,
        valuation_review_reports=valuation_reports,
        risk_event_occurrence_review_reports=risk_reports,
    )
    coverage_path = write_backtest_input_coverage_csv(
        result,
        tmp_path / "input_coverage.csv",
    )
    frame = pd.read_csv(coverage_path)

    record_types = set(frame["record_type"])
    assert {
        "component_coverage",
        "input_source_url",
        "risk_event_evidence_url",
        "ticker_input",
        "ticker_sec_feature",
        "valuation_source_type",
        "risk_event_source_type",
    }.issubset(record_types)
    ticker_feature = frame.loc[
        (frame["record_type"] == "ticker_sec_feature")
        & (frame["ticker"] == "NVDA")
        & (frame["feature_id"] == "gross_margin")
        & (frame["period_type"] == "quarterly")
    ]
    assert int(ticker_feature.iloc[0]["count"]) == 2
    risk_url = frame.loc[
        (frame["record_type"] == "risk_event_evidence_url")
        & (frame["event_id"] == "taiwan_geopolitical_escalation")
    ]
    assert "https://policy.example/release" in set(risk_url["source_url"])
    assert "True" in set(risk_url["score_eligible"].astype(str))
    assert backtest_input_coverage_records(result)


def test_backtest_report_includes_data_quality_gate_summary(tmp_path: Path) -> None:
    universe = load_universe()
    quality_report = replace(
        _quality_report(),
        price_summary=DataFileSummary(
            path=Path("prices.csv"),
            exists=True,
            rows=100,
            sha256="price-checksum",
            min_date=date(2026, 1, 1),
            max_date=date(2026, 4, 30),
        ),
        rate_summary=DataFileSummary(
            path=Path("rates.csv"),
            exists=True,
            rows=80,
            sha256="rate-checksum",
            min_date=date(2026, 1, 1),
            max_date=date(2026, 4, 30),
        ),
        issues=(
            DataQualityIssue(
                severity=Severity.WARNING,
                code="prices_stale",
                message="价格数据距离评估日偏旧。",
                rows=1,
                sample="SPY",
            ),
        ),
    )
    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=quality_report,
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 3),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
    )
    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 数据质量门禁摘要" in markdown
    assert "- 错误数：0；警告数：1" in markdown
    assert (
        "| 价格数据 | `prices.csv` | 100 | 2026-01-01 至 2026-04-30 | "
        "price-checksum |"
    ) in markdown
    assert "| 警告 | prices_stale | 1 | 价格数据距离评估日偏旧。 | SPY |" in markdown


def test_render_backtest_report_includes_component_coverage_summary(tmp_path: Path) -> None:
    universe = load_universe()
    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
    )
    first_row, second_row = result.rows[:2]
    result = replace(
        result,
        rows=(
            replace(
                first_row,
                component_coverages={"trend": 1.0, "fundamentals": 0.0},
            ),
            replace(
                second_row,
                component_coverages={"trend": 0.5, "fundamentals": 1.0},
            ),
        ),
    )

    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 模块覆盖率摘要" in markdown
    assert "| 模块 | 样本数 | 最低覆盖率 | 平均覆盖率 | 最高覆盖率 |" in markdown
    assert "| 趋势（trend） | 2 | 50% | 75% | 100% |" in markdown
    assert "| 基本面（fundamentals） | 2 | 0% | 50% | 100% |" in markdown


def test_render_backtest_report_includes_monthly_component_coverage_trend(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
    )
    first_row, second_row, third_row, fourth_row = result.rows[:4]
    result = replace(
        result,
        rows=(
            replace(
                first_row,
                signal_date=date(2026, 4, 15),
                component_coverages={"trend": 1.0, "fundamentals": 0.0},
            ),
            replace(
                second_row,
                signal_date=date(2026, 4, 30),
                component_coverages={"fundamentals": 0.5, "trend": 1.0},
            ),
            replace(
                third_row,
                signal_date=date(2026, 5, 1),
                component_coverages={"trend": 0.25, "fundamentals": 1.0},
            ),
            replace(
                fourth_row,
                signal_date=date(2026, 5, 2),
                component_coverages={"fundamentals": 1.0, "trend": 0.75},
            ),
        ),
    )

    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 月度模块覆盖率趋势" in markdown
    assert "| 月份 | 模块 | 样本数 | 平均覆盖率 | 最低覆盖率 | 覆盖不足天数 |" in markdown
    april_fundamentals = "| 2026-04 | 基本面（fundamentals） | 2 | 25% | 0% | 2 |"
    april_trend = "| 2026-04 | 趋势（trend） | 2 | 100% | 100% | 0 |"
    may_fundamentals = "| 2026-05 | 基本面（fundamentals） | 2 | 100% | 100% | 0 |"
    may_trend = "| 2026-05 | 趋势（trend） | 2 | 50% | 25% | 2 |"
    assert april_fundamentals in markdown
    assert april_trend in markdown
    assert may_fundamentals in markdown
    assert may_trend in markdown
    assert markdown.index(april_fundamentals) < markdown.index(april_trend)
    assert markdown.index(april_trend) < markdown.index(may_fundamentals)
    assert markdown.index(may_fundamentals) < markdown.index(may_trend)


def test_render_backtest_report_includes_monthly_component_source_type_trend(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
    )
    first_row, second_row, third_row, fourth_row = result.rows[:4]
    result = replace(
        result,
        rows=(
            replace(
                first_row,
                signal_date=date(2026, 4, 15),
                component_source_types={
                    "trend": "hard_data",
                    "fundamentals": "insufficient_data",
                },
            ),
            replace(
                second_row,
                signal_date=date(2026, 4, 30),
                component_source_types={
                    "fundamentals": "hard_data",
                    "trend": "partial_hard_data",
                },
            ),
            replace(
                third_row,
                signal_date=date(2026, 5, 1),
                component_source_types={
                    "trend": "hard_data",
                    "fundamentals": "manual_input",
                },
            ),
            replace(
                fourth_row,
                signal_date=date(2026, 5, 2),
                component_source_types={
                    "fundamentals": "partial_manual_input",
                    "trend": "placeholder",
                },
            ),
        ),
    )

    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 月度模块来源类型趋势" in markdown
    assert "| 月份 | 模块 | 样本数 | 来源类型分布 | 需解释天数 |" in markdown
    april_fundamentals = (
        "| 2026-04 | 基本面（fundamentals） | 2 | 硬数据 1；数据不足 1 | 1 |"
    )
    april_trend = "| 2026-04 | 趋势（trend） | 2 | 硬数据 1；部分硬数据 1 | 1 |"
    may_fundamentals = (
        "| 2026-05 | 基本面（fundamentals） | 2 | 手工/审计输入 1；"
        "部分手工/审计输入 1 | 1 |"
    )
    may_trend = "| 2026-05 | 趋势（trend） | 2 | 硬数据 1；占位输入 1 | 1 |"
    assert april_fundamentals in markdown
    assert april_trend in markdown
    assert may_fundamentals in markdown
    assert may_trend in markdown


def test_backtest_report_includes_monthly_input_issue_drilldown(tmp_path: Path) -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)
    signal_dates = [date(2026, 4, day) for day in range(1, 5)]
    fundamental_reports = {
        signal_date: _fundamental_feature_report(signal_date)
        for signal_date in signal_dates
    }
    valuation_reports = {
        signal_date: _valuation_review_report(signal_date)
        for signal_date in signal_dates
    }
    risk_reports = {
        signal_date: _risk_event_occurrence_review_report(signal_date)
        for signal_date in signal_dates
    }
    sec_issue = SecMetricIssue(
        severity=SecMetricIssueSeverity.WARNING,
        code="sec_fundamental_metrics_coverage_incomplete",
        message="TSM quarterly revenue 缺失。",
        ticker="TSM",
        metric_id="revenue",
        period_type="quarterly",
    )
    for signal_date in signal_dates[:2]:
        report = fundamental_reports[signal_date]
        fundamental_reports[signal_date] = replace(
            report,
            validation_report=replace(
                report.validation_report,
                issues=(sec_issue,),
                missing_observation_keys=(("TSM", "revenue", "quarterly"),),
            ),
        )
    valuation_report = valuation_reports[date(2026, 4, 3)]
    valuation_reports[date(2026, 4, 3)] = replace(
        valuation_report,
        validation_report=replace(
            valuation_report.validation_report,
            issues=(
                ValuationIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="stale_snapshot",
                    message="估值快照过期。",
                    snapshot_id="nvda_valuation_2026-04-03",
                    ticker="NVDA",
                ),
            ),
        ),
    )
    risk_report = risk_reports[date(2026, 4, 4)]
    risk_reports[date(2026, 4, 4)] = replace(
        risk_report,
        validation_report=replace(
            risk_report.validation_report,
            issues=(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.WARNING,
                    code="active_occurrence_stale",
                    message="风险事件证据过期。",
                    event_id="taiwan_geopolitical_escalation",
                    level="L3",
                ),
            ),
        ),
    )

    result = run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 5),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        fundamental_feature_reports=fundamental_reports,
        valuation_review_reports=valuation_reports,
        risk_event_occurrence_review_reports=risk_reports,
    )
    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 月度输入问题下钻" in markdown
    assert "| 月份 | 输入 | Code | 影响对象 | 次数 |" in markdown
    assert (
        "| 2026-04 | SEC 基本面 | sec_fundamental_metrics_missing_observation | "
        "TSM:revenue:quarterly | 2 |"
    ) in markdown
    assert (
        "| 2026-04 | 估值快照 | stale_snapshot | NVDA:nvda_valuation_2026-04-03 | "
        "1 |"
    ) in markdown
    assert (
        "| 2026-04 | 风险事件发生记录 | active_occurrence_stale | "
        "taiwan_geopolitical_escalation:L3 | 1 |"
    ) in markdown


def test_backtest_report_includes_monthly_risk_event_source_type_summary(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    signal_dates = [date(2026, 4, day) for day in range(1, 5)]
    risk_reports = {
        signal_dates[0]: _risk_event_occurrence_review_report(
            signal_dates[0],
            source_types=("manual_input",),
        ),
        signal_dates[1]: _risk_event_occurrence_review_report(
            signal_dates[1],
            source_types=("primary_source", "manual_input"),
        ),
        signal_dates[2]: _risk_event_occurrence_review_report(
            signal_dates[2],
            source_types=("public_convenience",),
        ),
        signal_dates[3]: _risk_event_occurrence_review_report(
            signal_dates[3],
            source_types=("paid_vendor",),
        ),
    }

    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 5),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        risk_event_occurrence_review_reports=risk_reports,
    )
    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 月度风险事件证据来源" in markdown
    assert "| 月份 | Source Type | 记录次数 |" in markdown
    assert "| 2026-04 | 手工/审计输入（manual_input） | 2 |" in markdown
    assert "| 2026-04 | 一手来源（primary_source） | 1 |" in markdown
    assert "| 2026-04 | 公开便利源（public_convenience） | 1 |" in markdown
    assert "| 2026-04 | 付费供应商（paid_vendor） | 1 |" in markdown


def test_backtest_report_includes_monthly_valuation_source_type_summary(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    signal_dates = [date(2026, 4, day) for day in range(1, 5)]
    valuation_reports = {
        signal_dates[0]: _valuation_review_report(
            signal_dates[0],
            source_type="manual_input",
        ),
        signal_dates[1]: _valuation_review_report(
            signal_dates[1],
            source_type="paid_vendor",
        ),
        signal_dates[2]: _valuation_review_report(
            signal_dates[2],
            source_type="primary_filing",
        ),
        signal_dates[3]: _valuation_review_report(
            signal_dates[3],
            source_type="public_convenience",
        ),
    }

    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 5),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        valuation_review_reports=valuation_reports,
    )
    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 月度估值快照来源" in markdown
    assert "| 月份 | Source Type | 快照数 |" in markdown
    assert "| 2026-04 | 手工/审计输入（manual_input） | 1 |" in markdown
    assert "| 2026-04 | 付费供应商（paid_vendor） | 1 |" in markdown
    assert "| 2026-04 | 一手披露（primary_filing） | 1 |" in markdown
    assert "| 2026-04 | 公开便利源（public_convenience） | 1 |" in markdown


def test_backtest_report_includes_monthly_input_source_url_summary(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    signal_dates = [date(2026, 4, day) for day in range(1, 3)]
    valuation_reports = {
        signal_dates[0]: _valuation_review_report(
            signal_dates[0],
            source_type="paid_vendor",
            source_url="https://vendor.example/nvda-valuation",
        ),
        signal_dates[1]: _valuation_review_report(
            signal_dates[1],
            source_type="paid_vendor",
            source_url="https://vendor.example/nvda-valuation",
        ),
    }
    risk_reports = {
        signal_dates[0]: _risk_event_occurrence_review_report(
            signal_dates[0],
            source_types=("primary_source",),
            source_url="https://policy.example/release",
        ),
        signal_dates[1]: _risk_event_occurrence_review_report(
            signal_dates[1],
            source_types=("primary_source",),
            source_url="https://policy.example/release",
        ),
    }

    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 3),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        valuation_review_reports=valuation_reports,
        risk_event_occurrence_review_reports=risk_reports,
    )
    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 月度输入证据 URL 摘要" in markdown
    assert "| 月份 | 输入 | Source Type | 影响对象 | Source URL | 次数 |" in markdown
    assert (
        "| 2026-04 | 估值快照 | 付费供应商（paid_vendor） | "
        "NVDA:nvda_valuation_2026-04-01 | https://vendor.example/nvda-valuation | 1 |"
    ) in markdown
    assert (
        "| 2026-04 | 风险事件发生记录 | 一手来源（primary_source） | "
        "taiwan_geopolitical_escalation:taiwan_geopolitical_escalation_2026-04-01 | "
        "https://policy.example/release | 1 |"
    ) in markdown
    assert "## 月度风险事件证据 URL 明细" in markdown
    assert (
        "| 月份 | Event | Occurrence | 状态 | 评分 | 相关 Ticker | "
        "Source Type | Source URL | 次数 |"
    ) in markdown
    assert (
        "| 2026-04 | taiwan_geopolitical_escalation | "
        "taiwan_geopolitical_escalation_2026-04-01 | 活跃（active） | 是 | "
        "TSM, NVDA, AMD, INTC, SMH, SOXX | 一手来源（primary_source） | "
        "https://policy.example/release | 1 |"
    ) in markdown


def test_backtest_report_includes_monthly_ticker_input_summary(
    tmp_path: Path,
) -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)
    signal_dates = [date(2026, 4, day) for day in range(1, 3)]
    fundamental_reports = {
        signal_date: _fundamental_feature_report(signal_date)
        for signal_date in signal_dates
    }
    for signal_date in signal_dates:
        report = fundamental_reports[signal_date]
        fundamental_reports[signal_date] = replace(
            report,
            validation_report=replace(
                report.validation_report,
                missing_observation_keys=(
                    ("NVDA", "revenue", "quarterly"),
                    ("NVDA", "gross_profit", "quarterly"),
                ),
            ),
        )
    valuation_reports = {
        signal_date: _valuation_review_report(signal_date)
        for signal_date in signal_dates
    }
    risk_reports = {
        signal_date: _risk_event_occurrence_review_report(
            signal_date,
            source_types=("manual_input",),
        )
        for signal_date in signal_dates
    }

    result = run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 3),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        fundamental_feature_reports=fundamental_reports,
        valuation_review_reports=valuation_reports,
        risk_event_occurrence_review_reports=risk_reports,
    )
    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=tmp_path / "daily.csv",
    )

    assert "## 月度 ticker 输入摘要" in markdown
    assert (
        "| 月份 | Ticker | SEC 特征行 | SEC 缺失观测 | 估值快照 | "
        "活跃/观察风险事件 | 可评分风险事件 |"
    ) in markdown
    assert "| 2026-04 | NVDA | 10 | 4 | 2 | 2 | 2 |" in markdown
    assert "| 2026-04 | MSFT | 10 | 0 | 0 | 0 | 0 |" in markdown
    assert "## 月度 ticker SEC 特征明细" in markdown
    assert "| 2026-04 | NVDA | gross_margin | quarterly | 2 |" in markdown
    assert "| 2026-04 | MSFT | capex_intensity | annual | 2 |" in markdown


def test_backtest_cli_writes_report_and_daily_csv(tmp_path: Path) -> None:
    universe = load_universe()
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    report_path = tmp_path / "backtest.md"
    daily_path = tmp_path / "backtest_daily.csv"
    input_coverage_path = tmp_path / "backtest_input_coverage.csv"
    audit_path = tmp_path / "backtest_audit.md"
    quality_path = tmp_path / "quality.md"
    watchlist_lifecycle_report_path = tmp_path / "watchlist_lifecycle.md"
    regimes_path = tmp_path / "market_regimes.yaml"
    sec_companies_path = tmp_path / "sec_companies.yaml"
    sec_metrics_path = tmp_path / "fundamental_metrics.yaml"
    sec_features_config_path = tmp_path / "fundamental_features.yaml"
    sec_companyfacts_dir = tmp_path / "sec_companyfacts"
    sec_companyfacts_validation_path = tmp_path / "sec_companyfacts_validation.md"
    valuation_dir = tmp_path / "valuation_snapshots"
    risk_occurrences_dir = tmp_path / "risk_event_occurrences"
    valuation_dir.mkdir()
    risk_occurrences_dir.mkdir()
    _sample_prices(configured_price_tickers(universe), periods=320).to_csv(
        prices_path,
        index=False,
    )
    _sample_rates(configured_rate_series(universe), periods=320).to_csv(
        rates_path,
        index=False,
    )
    regimes_path.write_text(
        "\n".join(
            [
                "default_backtest_regime: test_ai_regime",
                "regimes:",
                "  - regime_id: test_ai_regime",
                "    name: 测试 AI 行情",
                "    start_date: 2026-04-01",
                "    anchor_date: 2026-03-31",
                "    anchor_event: 测试锚定事件",
                "    description: 测试用市场阶段。",
                "    primary: true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    _write_sec_companies_config(sec_companies_path)
    _write_sec_metrics_config(sec_metrics_path)
    _write_sec_features_config(sec_features_config_path)
    _write_sec_companyfacts_cache(sec_companyfacts_dir)

    result = CliRunner().invoke(
        app,
        [
            "backtest",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--to",
            "2026-04-30",
            "--regimes-path",
            str(regimes_path),
            "--quality-as-of",
            "2026-05-02",
            "--report-path",
            str(report_path),
            "--daily-output-path",
            str(daily_path),
            "--input-coverage-output-path",
            str(input_coverage_path),
            "--audit-output-path",
            str(audit_path),
            "--quality-report-path",
            str(quality_path),
            "--benchmarks",
            "SPY,QQQ",
            "--slippage-bps",
            "2.5",
            "--sec-companies-path",
            str(sec_companies_path),
            "--sec-metrics-path",
            str(sec_metrics_path),
            "--fundamental-feature-config-path",
            str(sec_features_config_path),
            "--sec-companyfacts-dir",
            str(sec_companyfacts_dir),
            "--sec-companyfacts-validation-report-path",
            str(sec_companyfacts_validation_path),
            "--valuation-path",
            str(valuation_dir),
            "--risk-event-occurrences-path",
            str(risk_occurrences_dir),
            "--watchlist-lifecycle-report-path",
            str(watchlist_lifecycle_report_path),
        ],
    )

    assert result.exit_code == 0
    assert report_path.exists()
    assert daily_path.exists()
    assert input_coverage_path.exists()
    assert audit_path.exists()
    assert watchlist_lifecycle_report_path.exists()
    trace_path = tmp_path / "evidence" / "backtest_trace.json"
    assert trace_path.exists()
    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    claim_ids = {claim["claim_id"] for claim in trace["claims"]}
    assert "backtest:2026-04-01:2026-04-30:performance" in claim_ids
    assert quality_path.exists()
    assert sec_companyfacts_validation_path.exists()
    assert "回测状态：" in result.output
    assert "输入审计状态：" in result.output
    assert "SEC 基本面切片：" in result.output
    assert "历史输入覆盖诊断：" in result.output
    assert "市场阶段：测试 AI 行情" in result.output
    report_text = report_path.read_text(encoding="utf-8")
    assert "测试 AI 行情" in report_text
    assert "线性滑点/盘口冲击估算：2.5 bps" in report_text
    assert "SEC 基本面质量摘要" in report_text
    assert "## 可追溯引用" in report_text
    assert "backtest:2026-04-01:2026-04-30:performance" in report_text
    assert str(input_coverage_path) in report_text
    assert str(audit_path) in report_text
    audit_text = audit_path.read_text(encoding="utf-8")
    assert "# 回测输入审计报告" in audit_text
    assert str(report_path) in audit_text

    strict_audit_path = tmp_path / "backtest_audit_strict.md"
    strict_result = CliRunner().invoke(
        app,
        [
            "backtest",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--to",
            "2026-04-30",
            "--regimes-path",
            str(regimes_path),
            "--quality-as-of",
            "2026-05-02",
            "--report-path",
            str(tmp_path / "backtest_strict.md"),
            "--daily-output-path",
            str(tmp_path / "backtest_daily_strict.csv"),
            "--input-coverage-output-path",
            str(tmp_path / "backtest_input_coverage_strict.csv"),
            "--audit-output-path",
            str(strict_audit_path),
            "--quality-report-path",
            str(tmp_path / "quality_strict.md"),
            "--benchmarks",
            "SPY,QQQ",
            "--sec-companies-path",
            str(sec_companies_path),
            "--sec-metrics-path",
            str(sec_metrics_path),
            "--fundamental-feature-config-path",
            str(sec_features_config_path),
            "--sec-companyfacts-dir",
            str(sec_companyfacts_dir),
            "--sec-companyfacts-validation-report-path",
            str(tmp_path / "sec_companyfacts_validation_strict.md"),
            "--valuation-path",
            str(valuation_dir),
            "--risk-event-occurrences-path",
            str(risk_occurrences_dir),
            "--watchlist-lifecycle-report-path",
            str(tmp_path / "watchlist_lifecycle_strict.md"),
            "--fail-on-audit-warning",
        ],
    )

    assert strict_result.exit_code == 1
    assert strict_audit_path.exists()
    assert "严格审计门禁已返回失败" in strict_result.output
    lookup_result = CliRunner().invoke(
        app,
        [
            "trace",
            "lookup",
            "--bundle-path",
            str(trace_path),
            "--id",
            "backtest:2026-04-01:2026-04-30:performance",
        ],
    )
    assert lookup_result.exit_code == 0
    assert "策略总收益" in lookup_result.output


def _quality_report() -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-05-01T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 5, 2),
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=1),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1),
        expected_price_tickers=("SPY",),
        expected_rate_series=("DGS10",),
        issues=(),
    )


def _fundamental_feature_report(as_of: date) -> SecFundamentalFeaturesReport:
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
        input_path=Path("point_in_time_metrics.csv"),
        validation_report=SecFundamentalMetricsCsvValidationReport(
            as_of=as_of,
            input_path=Path("point_in_time_metrics.csv"),
            row_count=20,
            as_of_row_count=20,
            expected_observation_count=20,
            observed_observation_count=20,
        ),
        rows=rows,
    )


def _valuation_review_report(
    as_of: date,
    source_type: ValuationSourceType = "manual_input",
    source_url: str = "",
) -> ValuationReviewReport:
    validation_report = ValuationValidationReport(
        as_of=as_of,
        input_path=Path("valuation_snapshots"),
        snapshots=(
            LoadedValuationSnapshot(
                snapshot=ValuationSnapshot(
                    snapshot_id=f"nvda_valuation_{as_of.isoformat()}",
                    ticker="NVDA",
                    as_of=as_of,
                    source_type=source_type,
                    source_name=f"{source_type}_sheet",
                    source_url=source_url,
                    captured_at=as_of,
                    valuation_metrics=(
                        [
                            SnapshotMetric(
                                metric_id="forward_pe",
                                value=36.0,
                                unit="ratio",
                                period="next_12m",
                            )
                        ]
                    ),
                    valuation_percentile=82.0,
                    overall_assessment="expensive",
                ),
                path=Path("nvda_valuation.yaml"),
            ),
        ),
    )
    return build_valuation_review_report(validation_report)


def _risk_event_occurrence_review_report(
    as_of: date,
    source_types: tuple[RiskEventOccurrenceSourceType, ...] = ("manual_input",),
    source_url: str = "",
) -> RiskEventOccurrenceReviewReport:
    validation_report = RiskEventOccurrenceValidationReport(
        as_of=as_of,
        input_path=Path("risk_event_occurrences"),
        config=load_risk_events(),
        occurrences=(
            LoadedRiskEventOccurrence(
                occurrence=RiskEventOccurrence(
                    occurrence_id=f"taiwan_geopolitical_escalation_{as_of.isoformat()}",
                    event_id="taiwan_geopolitical_escalation",
                    status="active",
                    triggered_at=as_of,
                    last_confirmed_at=as_of,
                    evidence_sources=[
                        RiskEventEvidenceSource(
                            source_name=f"{source_type}_review",
                            source_type=source_type,
                            source_url=source_url,
                            captured_at=as_of,
                        )
                        for source_type in source_types
                    ],
                    summary="测试用 L3 风险事件。",
                ),
                path=Path("risk_event_occurrences/taiwan.yaml"),
            ),
        ),
    )
    return build_risk_event_occurrence_review_report(validation_report)


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
        source_path=Path("point_in_time_metrics.csv"),
    )


def _sample_prices(tickers: list[str], periods: int) -> pd.DataFrame:
    dates = pd.date_range(end="2026-04-30", periods=periods, freq="D")
    rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        base = 100.0 + ticker_index * 10.0
        daily_step = 0.2 + ticker_index * 0.01
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
      - dei
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


def _write_sec_companyfacts_cache(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "nvda_companyfacts.json"
    json_path.write_text(
        json.dumps(
            {
                "cik": 1045810,
                "entityName": "NVIDIA Corporation",
                "facts": {
                    "us-gaap": {
                        "Revenues": {
                            "units": {
                                "USD": [
                                    {
                                        "fy": 2025,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "end": "2026-01-31",
                                        "filed": "2026-02-27",
                                        "val": 1000,
                                        "accn": "0001045810-26-000001",
                                    }
                                ]
                            }
                        },
                        "GrossProfit": {
                            "units": {
                                "USD": [
                                    {
                                        "fy": 2025,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "end": "2026-01-31",
                                        "filed": "2026-02-27",
                                        "val": 650,
                                        "accn": "0001045810-26-000001",
                                    }
                                ]
                            }
                        },
                    },
                    "dei": {},
                },
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "ticker": "NVDA",
                "cik": "0001045810",
                "checksum_sha256": _sha256(json_path),
            }
        ]
    ).to_csv(output_dir / "sec_companyfacts_manifest.csv", index=False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
