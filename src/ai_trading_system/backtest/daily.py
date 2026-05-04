from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import pandas as pd

from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.benchmark_policy import (
    BenchmarkPolicyReport,
    render_benchmark_policy_summary_section,
)
from ai_trading_system.config import FeatureConfig, PortfolioConfig, ScoringRulesConfig
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.features.market import build_market_features
from ai_trading_system.fundamentals.sec_features import SecFundamentalFeaturesReport
from ai_trading_system.risk_events import RiskEventOccurrenceReviewReport
from ai_trading_system.scoring.daily import (
    COMPONENT_LABELS,
    SOURCE_TYPE_LABELS,
    build_daily_score_report,
)
from ai_trading_system.valuation import ValuationReviewReport
from ai_trading_system.watchlist_lifecycle import (
    WatchlistLifecycleConfig,
    active_watchlist_tickers_as_of,
)

DEFAULT_BENCHMARK_TICKERS = ("SPY", "QQQ", "SMH", "SOXX")
_SOURCE_TYPE_ORDER = (
    "hard_data",
    "partial_hard_data",
    "manual_input",
    "partial_manual_input",
    "insufficient_data",
    "placeholder",
    "derived",
)
_SOURCE_TYPES_REQUIRING_EXPLANATION = frozenset(
    {"partial_hard_data", "partial_manual_input", "insufficient_data", "placeholder"}
)
_RISK_EVENT_SOURCE_TYPE_LABELS = {
    "primary_source": "一手来源",
    "paid_vendor": "付费供应商",
    "manual_input": "手工/审计输入",
    "public_convenience": "公开便利源",
}
_VALUATION_SOURCE_TYPE_LABELS = {
    "primary_filing": "一手披露",
    "paid_vendor": "付费供应商",
    "manual_input": "手工/审计输入",
    "public_convenience": "公开便利源",
}
BACKTEST_INPUT_COVERAGE_COLUMNS = (
    "record_type",
    "month",
    "signal_date",
    "input",
    "ticker",
    "component",
    "feature_id",
    "period_type",
    "event_id",
    "occurrence_id",
    "status",
    "score_eligible",
    "related_tickers",
    "source_type",
    "source_url",
    "issue_code",
    "subject",
    "metric",
    "count",
    "value",
)


@dataclass(frozen=True)
class BacktestRegimeContext:
    regime_id: str
    name: str
    start_date: date
    anchor_date: date
    anchor_event: str
    description: str


@dataclass(frozen=True)
class BacktestDailyRow:
    signal_date: date
    return_date: date
    total_score: float
    confidence_score: float
    confidence_level: str
    position_label: str
    model_target_exposure: float
    gated_target_exposure: float
    raw_target_exposure: float
    target_exposure: float
    asset_return: float
    gross_return: float
    turnover: float
    commission_cost: float
    slippage_cost: float
    transaction_cost: float
    strategy_return: float
    strategy_equity: float
    component_scores: dict[str, float]
    component_source_types: dict[str, str]
    component_coverages: dict[str, float]
    position_gate_caps: dict[str, float]
    position_gate_triggers: dict[str, bool]

    def to_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "signal_date": self.signal_date.isoformat(),
            "return_date": self.return_date.isoformat(),
            "total_score": self.total_score,
            "confidence_score": self.confidence_score,
            "confidence_level": self.confidence_level,
            "position_label": self.position_label,
            "model_target_exposure": self.model_target_exposure,
            "gated_target_exposure": self.gated_target_exposure,
            "raw_target_exposure": self.raw_target_exposure,
            "target_exposure": self.target_exposure,
            "asset_return": self.asset_return,
            "gross_return": self.gross_return,
            "turnover": self.turnover,
            "commission_cost": self.commission_cost,
            "slippage_cost": self.slippage_cost,
            "transaction_cost": self.transaction_cost,
            "strategy_return": self.strategy_return,
            "strategy_equity": self.strategy_equity,
        }
        for component, score in self.component_scores.items():
            record[f"{component}_score"] = score
        for component, source_type in self.component_source_types.items():
            record[f"{component}_source_type"] = source_type
        for component, coverage in self.component_coverages.items():
            record[f"{component}_coverage"] = coverage
        for gate_id, cap in self.position_gate_caps.items():
            record[f"{gate_id}_gate_cap"] = cap
        for gate_id, triggered in self.position_gate_triggers.items():
            record[f"{gate_id}_gate_triggered"] = triggered
        return record


@dataclass(frozen=True)
class DailyBacktestResult:
    requested_start: date
    requested_end: date
    first_signal_date: date
    last_signal_date: date
    first_return_date: date
    last_return_date: date
    strategy_ticker: str
    benchmark_tickers: tuple[str, ...]
    cost_bps: float
    slippage_bps: float
    minimum_action_delta: float
    data_quality_report: DataQualityReport
    rows: tuple[BacktestDailyRow, ...]
    strategy_metrics: BacktestMetrics
    benchmark_metrics: dict[str, BacktestMetrics]
    market_regime: BacktestRegimeContext | None = None
    fundamental_feature_report_count: int = 0
    fundamental_feature_status_counts: dict[str, int] | None = None
    fundamental_feature_warning_count: int = 0
    fundamental_feature_row_count_min: int | None = None
    fundamental_feature_row_count_max: int | None = None
    valuation_review_report_count: int = 0
    valuation_review_status_counts: dict[str, int] | None = None
    valuation_review_warning_count: int = 0
    valuation_snapshot_count_min: int | None = None
    valuation_snapshot_count_max: int | None = None
    risk_event_occurrence_review_report_count: int = 0
    risk_event_occurrence_status_counts: dict[str, int] | None = None
    risk_event_occurrence_warning_count: int = 0
    risk_event_occurrence_count_min: int | None = None
    risk_event_occurrence_count_max: int | None = None
    risk_event_score_eligible_count_min: int | None = None
    risk_event_score_eligible_count_max: int | None = None
    monthly_input_issue_counts: dict[tuple[str, str, str, str], int] | None = None
    monthly_input_source_url_counts: dict[tuple[str, str, str, str, str], int] | None = None
    monthly_ticker_input_counts: dict[tuple[str, str, str], int] | None = None
    monthly_ticker_feature_counts: dict[tuple[str, str, str, str], int] | None = None
    monthly_risk_event_evidence_url_counts: (
        dict[tuple[str, str, str, str, str, str, str, str], int] | None
    ) = None
    monthly_valuation_source_type_counts: dict[tuple[str, str], int] | None = None
    monthly_risk_event_source_type_counts: dict[tuple[str, str], int] | None = None
    benchmark_policy_report: BenchmarkPolicyReport | None = None

    @property
    def status(self) -> str:
        return "PASS_WITH_LIMITATIONS"


def run_daily_score_backtest(
    prices: pd.DataFrame,
    rates: pd.DataFrame,
    feature_config: FeatureConfig,
    scoring_rules: ScoringRulesConfig,
    portfolio_config: PortfolioConfig,
    data_quality_report: DataQualityReport,
    core_watchlist: list[str],
    start: date,
    end: date,
    strategy_ticker: str = "SMH",
    benchmark_tickers: tuple[str, ...] = DEFAULT_BENCHMARK_TICKERS,
    cost_bps: float = 5.0,
    slippage_bps: float = 0.0,
    market_regime: BacktestRegimeContext | None = None,
    fundamental_feature_reports: Mapping[date, SecFundamentalFeaturesReport] | None = None,
    valuation_review_reports: Mapping[date, ValuationReviewReport] | None = None,
    risk_event_occurrence_review_reports: (
        Mapping[date, RiskEventOccurrenceReviewReport] | None
    ) = None,
    watchlist_lifecycle: WatchlistLifecycleConfig | None = None,
    benchmark_policy_report: BenchmarkPolicyReport | None = None,
) -> DailyBacktestResult:
    if start >= end:
        raise ValueError("回测开始日期必须早于结束日期")
    if cost_bps < 0:
        raise ValueError("交易成本 bps 不能为负数")
    if slippage_bps < 0:
        raise ValueError("滑点 bps 不能为负数")

    close_pivot = _prepare_adjusted_close_pivot(prices)
    _check_required_tickers(close_pivot, (strategy_ticker, *benchmark_tickers))
    periods = _backtest_periods(close_pivot[strategy_ticker], start, end)
    if not periods:
        raise ValueError("回测区间内没有可用的下一交易日收益")

    cost_rate = cost_bps / 10_000.0
    slippage_rate = slippage_bps / 10_000.0
    previous_exposure = 0.0
    running_equity = 1.0
    rows: list[BacktestDailyRow] = []
    fundamental_feature_report_count = 0
    fundamental_feature_status_counts: dict[str, int] = {}
    fundamental_feature_warning_count = 0
    fundamental_feature_row_counts: list[int] = []
    valuation_review_report_count = 0
    valuation_review_status_counts: dict[str, int] = {}
    valuation_review_warning_count = 0
    valuation_snapshot_counts: list[int] = []
    risk_event_occurrence_review_report_count = 0
    risk_event_occurrence_status_counts: dict[str, int] = {}
    risk_event_occurrence_warning_count = 0
    risk_event_occurrence_counts: list[int] = []
    risk_event_score_eligible_counts: list[int] = []
    monthly_input_issue_counts: Counter[tuple[str, str, str, str]] = Counter()
    monthly_input_source_url_counts: Counter[tuple[str, str, str, str, str]] = Counter()
    monthly_ticker_input_counts: Counter[tuple[str, str, str]] = Counter()
    monthly_ticker_feature_counts: Counter[tuple[str, str, str, str]] = Counter()
    monthly_risk_event_evidence_url_counts: Counter[
        tuple[str, str, str, str, str, str, str, str]
    ] = Counter()
    monthly_valuation_source_type_counts: Counter[tuple[str, str]] = Counter()
    monthly_risk_event_source_type_counts: Counter[tuple[str, str]] = Counter()

    for signal_date, return_date in periods:
        fundamental_feature_report = None
        if fundamental_feature_reports is not None:
            fundamental_feature_report = fundamental_feature_reports.get(signal_date)
            if fundamental_feature_report is None:
                raise ValueError(
                    "回测缺少 point-in-time SEC 基本面特征："
                    f"{signal_date.isoformat()}"
                )
            fundamental_feature_report_count += 1
            fundamental_feature_status_counts[fundamental_feature_report.status] = (
                fundamental_feature_status_counts.get(fundamental_feature_report.status, 0) + 1
            )
            fundamental_feature_warning_count += fundamental_feature_report.warning_count
            fundamental_feature_row_counts.append(fundamental_feature_report.row_count)
            sec_missing_observations = (
                fundamental_feature_report.validation_report.missing_observation_keys
            )
            _record_monthly_sec_missing_observations(
                monthly_input_issue_counts,
                monthly_ticker_input_counts,
                signal_date=signal_date,
                missing_observation_keys=sec_missing_observations,
            )
            _record_monthly_sec_feature_inputs(
                monthly_ticker_input_counts,
                monthly_ticker_feature_counts,
                signal_date=signal_date,
                report=fundamental_feature_report,
            )
            sec_validation_issues = fundamental_feature_report.validation_report.issues
            if sec_missing_observations:
                sec_validation_issues = tuple(
                    issue
                    for issue in sec_validation_issues
                    if issue.code != "sec_fundamental_metrics_coverage_incomplete"
                )
            _record_monthly_input_issues(
                monthly_input_issue_counts,
                signal_date=signal_date,
                input_label="SEC 基本面",
                issues=sec_validation_issues,
            )
            _record_monthly_input_issues(
                monthly_input_issue_counts,
                signal_date=signal_date,
                input_label="SEC 基本面",
                issues=fundamental_feature_report.issues,
            )
        valuation_review_report = None
        if valuation_review_reports is not None:
            valuation_review_report = valuation_review_reports.get(signal_date)
            if valuation_review_report is None:
                raise ValueError(
                    "回测缺少 point-in-time 估值快照复核报告："
                    f"{signal_date.isoformat()}"
                )
            valuation_review_report_count += 1
            valuation_review_status_counts[valuation_review_report.status] = (
                valuation_review_status_counts.get(valuation_review_report.status, 0) + 1
            )
            valuation_review_warning_count += (
                valuation_review_report.validation_report.warning_count
            )
            valuation_snapshot_counts.append(
                valuation_review_report.validation_report.snapshot_count
            )
            _record_monthly_valuation_source_types(
                monthly_valuation_source_type_counts,
                signal_date=signal_date,
                report=valuation_review_report,
            )
            _record_monthly_valuation_ticker_inputs(
                monthly_ticker_input_counts,
                signal_date=signal_date,
                report=valuation_review_report,
            )
            _record_monthly_valuation_source_urls(
                monthly_input_source_url_counts,
                signal_date=signal_date,
                report=valuation_review_report,
            )
            _record_monthly_input_issues(
                monthly_input_issue_counts,
                signal_date=signal_date,
                input_label="估值快照",
                issues=valuation_review_report.validation_report.issues,
            )
        risk_event_occurrence_review_report = None
        if risk_event_occurrence_review_reports is not None:
            risk_event_occurrence_review_report = risk_event_occurrence_review_reports.get(
                signal_date
            )
            if risk_event_occurrence_review_report is None:
                raise ValueError(
                    "回测缺少 point-in-time 风险事件发生记录复核报告："
                    f"{signal_date.isoformat()}"
                )
            risk_event_occurrence_review_report_count += 1
            status = risk_event_occurrence_review_report.status
            risk_event_occurrence_status_counts[status] = (
                risk_event_occurrence_status_counts.get(status, 0) + 1
            )
            risk_event_occurrence_warning_count += (
                risk_event_occurrence_review_report.validation_report.warning_count
            )
            risk_event_occurrence_counts.append(
                risk_event_occurrence_review_report.validation_report.occurrence_count
            )
            risk_event_score_eligible_counts.append(
                len(risk_event_occurrence_review_report.score_eligible_active_items)
            )
            _record_monthly_risk_event_source_types(
                monthly_risk_event_source_type_counts,
                signal_date=signal_date,
                report=risk_event_occurrence_review_report,
            )
            _record_monthly_risk_event_ticker_inputs(
                monthly_ticker_input_counts,
                signal_date=signal_date,
                report=risk_event_occurrence_review_report,
            )
            _record_monthly_risk_event_source_urls(
                monthly_input_source_url_counts,
                signal_date=signal_date,
                report=risk_event_occurrence_review_report,
            )
            _record_monthly_risk_event_evidence_urls(
                monthly_risk_event_evidence_url_counts,
                signal_date=signal_date,
                report=risk_event_occurrence_review_report,
            )
            _record_monthly_input_issues(
                monthly_input_issue_counts,
                signal_date=signal_date,
                input_label="风险事件发生记录",
                issues=risk_event_occurrence_review_report.validation_report.issues,
            )
        effective_core_watchlist = (
            active_watchlist_tickers_as_of(
                lifecycle=watchlist_lifecycle,
                tickers=core_watchlist,
                as_of=signal_date,
            )
            if watchlist_lifecycle is not None
            else core_watchlist
        )
        feature_set = build_market_features(
            prices=prices,
            rates=rates,
            config=feature_config,
            as_of=signal_date,
            core_watchlist=effective_core_watchlist,
        )
        score_report = build_daily_score_report(
            feature_set=feature_set,
            data_quality_report=data_quality_report,
            rules=scoring_rules,
            total_risk_asset_min=portfolio_config.portfolio.total_risk_asset_min,
            total_risk_asset_max=portfolio_config.portfolio.total_risk_asset_max,
            max_total_ai_exposure=portfolio_config.position_limits.max_total_ai_exposure,
            fundamental_feature_report=fundamental_feature_report,
            valuation_review_report=valuation_review_report,
            risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        )
        recommendation = score_report.recommendation
        confidence = score_report.confidence_assessment
        model_target_exposure = _position_midpoint(
            recommendation.model_risk_asset_ai_band.min_position,
            recommendation.model_risk_asset_ai_band.max_position,
        )
        gated_target_exposure = _position_midpoint(
            recommendation.risk_asset_ai_band.min_position,
            recommendation.risk_asset_ai_band.max_position,
        )
        raw_target_exposure = gated_target_exposure
        if not rows or abs(gated_target_exposure - previous_exposure) >= (
            scoring_rules.position_change.minimum_action_delta
        ):
            target_exposure = gated_target_exposure
        else:
            target_exposure = previous_exposure

        asset_return = _period_return(close_pivot, strategy_ticker, signal_date, return_date)
        gross_return = target_exposure * asset_return
        turnover = abs(target_exposure - previous_exposure)
        commission_cost = turnover * cost_rate
        slippage_cost = turnover * slippage_rate
        transaction_cost = commission_cost + slippage_cost
        strategy_return = gross_return - transaction_cost
        running_equity *= 1.0 + strategy_return

        rows.append(
            BacktestDailyRow(
                signal_date=signal_date,
                return_date=return_date,
                total_score=recommendation.total_score,
                confidence_score=confidence.score,
                confidence_level=confidence.level,
                position_label=recommendation.label,
                model_target_exposure=model_target_exposure,
                gated_target_exposure=gated_target_exposure,
                raw_target_exposure=raw_target_exposure,
                target_exposure=target_exposure,
                asset_return=asset_return,
                gross_return=gross_return,
                turnover=turnover,
                commission_cost=commission_cost,
                slippage_cost=slippage_cost,
                transaction_cost=transaction_cost,
                strategy_return=strategy_return,
                strategy_equity=running_equity,
                component_scores={
                    component.name: component.score for component in score_report.components
                },
                component_source_types={
                    component.name: component.source_type for component in score_report.components
                },
                component_coverages={
                    component.name: component.coverage for component in score_report.components
                },
                position_gate_caps={
                    gate.gate_id: gate.max_position for gate in recommendation.position_gates
                },
                position_gate_triggers={
                    gate.gate_id: gate.triggered for gate in recommendation.position_gates
                },
            )
        )
        previous_exposure = target_exposure

    strategy_metrics = summarize_long_only_backtest(
        strategy_returns=[row.strategy_return for row in rows],
        exposures=[row.target_exposure for row in rows],
        turnovers=[row.turnover for row in rows],
    )
    benchmark_metrics = {
        ticker: _benchmark_metrics(close_pivot, ticker, periods) for ticker in benchmark_tickers
    }

    return DailyBacktestResult(
        requested_start=start,
        requested_end=end,
        first_signal_date=rows[0].signal_date,
        last_signal_date=rows[-1].signal_date,
        first_return_date=rows[0].return_date,
        last_return_date=rows[-1].return_date,
        strategy_ticker=strategy_ticker,
        benchmark_tickers=benchmark_tickers,
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        minimum_action_delta=scoring_rules.position_change.minimum_action_delta,
        data_quality_report=data_quality_report,
        rows=tuple(rows),
        strategy_metrics=strategy_metrics,
        benchmark_metrics=benchmark_metrics,
        market_regime=market_regime,
        fundamental_feature_report_count=fundamental_feature_report_count,
        fundamental_feature_status_counts=fundamental_feature_status_counts or None,
        fundamental_feature_warning_count=fundamental_feature_warning_count,
        fundamental_feature_row_count_min=(
            min(fundamental_feature_row_counts) if fundamental_feature_row_counts else None
        ),
        fundamental_feature_row_count_max=(
            max(fundamental_feature_row_counts) if fundamental_feature_row_counts else None
        ),
        valuation_review_report_count=valuation_review_report_count,
        valuation_review_status_counts=valuation_review_status_counts or None,
        valuation_review_warning_count=valuation_review_warning_count,
        valuation_snapshot_count_min=(
            min(valuation_snapshot_counts) if valuation_snapshot_counts else None
        ),
        valuation_snapshot_count_max=(
            max(valuation_snapshot_counts) if valuation_snapshot_counts else None
        ),
        risk_event_occurrence_review_report_count=risk_event_occurrence_review_report_count,
        risk_event_occurrence_status_counts=(
            risk_event_occurrence_status_counts or None
        ),
        risk_event_occurrence_warning_count=risk_event_occurrence_warning_count,
        risk_event_occurrence_count_min=(
            min(risk_event_occurrence_counts) if risk_event_occurrence_counts else None
        ),
        risk_event_occurrence_count_max=(
            max(risk_event_occurrence_counts) if risk_event_occurrence_counts else None
        ),
        risk_event_score_eligible_count_min=(
            min(risk_event_score_eligible_counts)
            if risk_event_score_eligible_counts
            else None
        ),
        risk_event_score_eligible_count_max=(
            max(risk_event_score_eligible_counts)
            if risk_event_score_eligible_counts
            else None
        ),
        monthly_input_issue_counts=dict(monthly_input_issue_counts) or None,
        monthly_input_source_url_counts=dict(monthly_input_source_url_counts) or None,
        monthly_ticker_input_counts=dict(monthly_ticker_input_counts) or None,
        monthly_ticker_feature_counts=dict(monthly_ticker_feature_counts) or None,
        monthly_risk_event_evidence_url_counts=(
            dict(monthly_risk_event_evidence_url_counts) or None
        ),
        monthly_valuation_source_type_counts=(
            dict(monthly_valuation_source_type_counts) or None
        ),
        monthly_risk_event_source_type_counts=(
            dict(monthly_risk_event_source_type_counts) or None
        ),
        benchmark_policy_report=benchmark_policy_report,
    )


def write_backtest_daily_csv(result: DailyBacktestResult, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row.to_record() for row in result.rows]).to_csv(output_path, index=False)
    return output_path


def write_backtest_input_coverage_csv(
    result: DailyBacktestResult,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        backtest_input_coverage_records(result),
        columns=BACKTEST_INPUT_COVERAGE_COLUMNS,
    ).to_csv(output_path, index=False)
    return output_path


def backtest_input_coverage_records(
    result: DailyBacktestResult,
) -> tuple[dict[str, object], ...]:
    records: list[dict[str, object]] = []
    records.extend(_component_input_coverage_records(result))
    records.extend(_input_issue_coverage_records(result))
    records.extend(_input_source_url_coverage_records(result))
    records.extend(_ticker_input_coverage_records(result))
    records.extend(_ticker_feature_coverage_records(result))
    records.extend(_risk_event_evidence_url_coverage_records(result))
    records.extend(_input_source_type_coverage_records(result))
    return tuple(records)


def render_backtest_report(
    result: DailyBacktestResult,
    data_quality_report_path: Path,
    daily_output_path: Path,
    sec_companyfacts_validation_report_path: Path | None = None,
    input_coverage_output_path: Path | None = None,
    audit_report_path: Path | None = None,
    traceability_section: str | None = None,
) -> str:
    lines = [
        "# 历史回测报告",
        "",
        f"- 状态：{result.status}",
        f"- 请求区间：{result.requested_start.isoformat()} 至 {result.requested_end.isoformat()}",
    ]
    if result.market_regime is not None:
        lines.extend(
            [
                (
                    f"- 市场阶段：{result.market_regime.name}"
                    f"（{result.market_regime.regime_id}）"
                ),
                f"- 阶段默认起点：{result.market_regime.start_date.isoformat()}",
                (
                    f"- 锚定事件：{result.market_regime.anchor_date.isoformat()} "
                    f"{result.market_regime.anchor_event}"
                ),
            ]
        )
        if result.requested_start != result.market_regime.start_date:
            lines.append(
                "- 起点说明：本次请求起点与市场阶段默认起点不同，"
                "结论应按实际请求区间解释。"
            )

    lines.extend(
        [
            (
                f"- 实际信号区间：{result.first_signal_date.isoformat()} "
                f"至 {result.last_signal_date.isoformat()}"
            ),
            (
                f"- 实际收益区间：{result.first_return_date.isoformat()} "
                f"至 {result.last_return_date.isoformat()}"
            ),
            f"- 策略代理标的：{result.strategy_ticker}",
            f"- 基准：{', '.join(result.benchmark_tickers)}",
            f"- 基准政策状态：{_benchmark_policy_status(result)}",
            f"- 单边交易成本：{result.cost_bps:.1f} bps",
            f"- 线性滑点/盘口冲击估算：{result.slippage_bps:.1f} bps",
            f"- 最小调仓阈值：{result.minimum_action_delta:.0%}",
            f"- 数据质量状态：{result.data_quality_report.status}",
            f"- 数据质量报告：`{data_quality_report_path}`",
            f"- SEC 基本面切片数：{result.fundamental_feature_report_count}",
            f"- 估值快照切片数：{result.valuation_review_report_count}",
            (
                "- 风险事件发生记录切片数："
                f"{result.risk_event_occurrence_review_report_count}"
            ),
            f"- 每日回测明细：`{daily_output_path}`",
        ]
    )
    if input_coverage_output_path is not None:
        lines.append(f"- 历史输入覆盖诊断：`{input_coverage_output_path}`")
    if audit_report_path is not None:
        lines.append(f"- 输入审计报告：`{audit_report_path}`")

    lines.extend(
        [
            "",
            "## 核心指标",
            "",
            "| 组合 | 总收益 | CAGR | 最大回撤 | Sharpe | Sortino | Calmar | 在场比例 | 换手 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
            _metrics_row(
                f"策略（{result.strategy_ticker} 动态仓位）",
                result.strategy_metrics,
            ),
        ]
    )

    for ticker in result.benchmark_tickers:
        lines.append(_metrics_row(f"基准（{ticker} 买入持有）", result.benchmark_metrics[ticker]))

    benchmark_policy_section = render_benchmark_policy_summary_section(
        result.benchmark_policy_report
    )
    if benchmark_policy_section:
        lines.extend(["", benchmark_policy_section.rstrip()])

    lines.extend(_execution_cost_summary_lines(result))
    position_gate_rows = _position_gate_summary_rows(result)
    if position_gate_rows:
        lines.extend(
            [
                "",
                "## 仓位闸门摘要",
                "",
                "| Gate | 样本数 | 触发天数 | 最低上限 | 平均上限 |",
                "|---|---:|---:|---:|---:|",
                *position_gate_rows,
            ]
        )
    confidence_rows = _confidence_summary_rows(result)
    if confidence_rows:
        lines.extend(
            [
                "",
                "## 判断置信度摘要",
                "",
                "| 置信度分桶 | 样本数 | 平均总分 | 平均最终仓位 | 平均收益 |",
                "|---|---:|---:|---:|---:|",
                *confidence_rows,
            ]
        )
    lines.extend(_data_quality_gate_summary_lines(result.data_quality_report))

    component_coverage_rows = _component_coverage_summary_rows(result)
    if component_coverage_rows:
        lines.extend(
            [
                "",
                "## 模块覆盖率摘要",
                "",
                "| 模块 | 样本数 | 最低覆盖率 | 平均覆盖率 | 最高覆盖率 |",
                "|---|---:|---:|---:|---:|",
                *component_coverage_rows,
            ]
        )
    monthly_component_coverage_rows = _monthly_component_coverage_trend_rows(result)
    if monthly_component_coverage_rows:
        lines.extend(
            [
                "",
                "## 月度模块覆盖率趋势",
                "",
                "| 月份 | 模块 | 样本数 | 平均覆盖率 | 最低覆盖率 | 覆盖不足天数 |",
                "|---|---|---:|---:|---:|---:|",
                *monthly_component_coverage_rows,
            ]
        )
    monthly_component_source_type_rows = _monthly_component_source_type_rows(result)
    if monthly_component_source_type_rows:
        lines.extend(
            [
                "",
                "## 月度模块来源类型趋势",
                "",
                "| 月份 | 模块 | 样本数 | 来源类型分布 | 需解释天数 |",
                "|---|---|---:|---|---:|",
                *monthly_component_source_type_rows,
            ]
        )
    monthly_input_issue_rows = _monthly_input_issue_rows(result)
    if monthly_input_issue_rows:
        lines.extend(
            [
                "",
                "## 月度输入问题下钻",
                "",
                "| 月份 | 输入 | Code | 影响对象 | 次数 |",
                "|---|---|---|---|---:|",
                *monthly_input_issue_rows,
            ]
        )
    monthly_input_source_url_rows = _monthly_input_source_url_rows(result)
    if monthly_input_source_url_rows:
        lines.extend(
            [
                "",
                "## 月度输入证据 URL 摘要",
                "",
                "| 月份 | 输入 | Source Type | 影响对象 | Source URL | 次数 |",
                "|---|---|---|---|---|---:|",
                *monthly_input_source_url_rows,
            ]
        )
    monthly_risk_event_evidence_url_rows = _monthly_risk_event_evidence_url_rows(result)
    if monthly_risk_event_evidence_url_rows:
        lines.extend(
            [
                "",
                "## 月度风险事件证据 URL 明细",
                "",
                (
                    "| 月份 | Event | Occurrence | 状态 | 评分 | 相关 Ticker | "
                    "Source Type | Source URL | 次数 |"
                ),
                "|---|---|---|---|---|---|---|---|---:|",
                *monthly_risk_event_evidence_url_rows,
            ]
        )
    monthly_ticker_input_rows = _monthly_ticker_input_rows(result)
    if monthly_ticker_input_rows:
        lines.extend(
            [
                "",
                "## 月度 ticker 输入摘要",
                "",
                (
                    "| 月份 | Ticker | SEC 特征行 | SEC 缺失观测 | 估值快照 | "
                    "活跃/观察风险事件 | 可评分风险事件 |"
                ),
                "|---|---|---:|---:|---:|---:|---:|",
                *monthly_ticker_input_rows,
            ]
        )
    monthly_ticker_feature_rows = _monthly_ticker_feature_rows(result)
    if monthly_ticker_feature_rows:
        lines.extend(
            [
                "",
                "## 月度 ticker SEC 特征明细",
                "",
                "| 月份 | Ticker | Feature | Period | 特征行数 |",
                "|---|---|---|---|---:|",
                *monthly_ticker_feature_rows,
            ]
        )
    monthly_valuation_source_type_rows = _monthly_valuation_source_type_rows(result)
    if monthly_valuation_source_type_rows:
        lines.extend(
            [
                "",
                "## 月度估值快照来源",
                "",
                "| 月份 | Source Type | 快照数 |",
                "|---|---|---:|",
                *monthly_valuation_source_type_rows,
            ]
        )
    monthly_risk_event_source_type_rows = _monthly_risk_event_source_type_rows(result)
    if monthly_risk_event_source_type_rows:
        lines.extend(
            [
                "",
                "## 月度风险事件证据来源",
                "",
                "| 月份 | Source Type | 记录次数 |",
                "|---|---|---:|",
                *monthly_risk_event_source_type_rows,
            ]
        )

    limitation_note = _backtest_limitation_note(result)
    if result.fundamental_feature_report_count:
        fundamental_row_count_range = _count_range(
            result.fundamental_feature_row_count_min,
            result.fundamental_feature_row_count_max,
        )
        lines.extend(
            [
                "",
                "## SEC 基本面质量摘要",
                "",
                (
                    "- 切片状态统计："
                    f"{_status_counts_summary(result.fundamental_feature_status_counts)}"
                ),
                f"- 特征行数范围：{fundamental_row_count_range}",
                f"- 特征警告数：{result.fundamental_feature_warning_count}",
            ]
        )
    if result.valuation_review_report_count:
        valuation_snapshot_count_range = _count_range(
            result.valuation_snapshot_count_min,
            result.valuation_snapshot_count_max,
        )
        lines.extend(
            [
                "",
                "## 估值快照质量摘要",
                "",
                (
                    "- 切片状态统计："
                    f"{_status_counts_summary(result.valuation_review_status_counts)}"
                ),
                f"- 快照数量范围：{valuation_snapshot_count_range}",
                f"- 校验警告数：{result.valuation_review_warning_count}",
            ]
        )
    if result.risk_event_occurrence_review_report_count:
        risk_event_occurrence_count_range = _count_range(
            result.risk_event_occurrence_count_min,
            result.risk_event_occurrence_count_max,
        )
        risk_event_score_eligible_count_range = _count_range(
            result.risk_event_score_eligible_count_min,
            result.risk_event_score_eligible_count_max,
        )
        lines.extend(
            [
                "",
                "## 风险事件质量摘要",
                "",
                (
                    "- 切片状态统计："
                    f"{_status_counts_summary(result.risk_event_occurrence_status_counts)}"
                ),
                f"- 发生记录数量范围：{risk_event_occurrence_count_range}",
                f"- 可评分活跃/观察记录范围：{risk_event_score_eligible_count_range}",
                f"- 校验警告数：{result.risk_event_occurrence_warning_count}",
            ]
        )

    if traceability_section is not None:
        lines.append(traceability_section)

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 每个交易日收盘后计算评分，目标仓位从下一交易日收益开始生效，避免未来函数。",
            (
                "- 评分模型仓位先经过仓位闸门取最严格上限，"
                "再使用最终 AI 仓位区间中点；变化小于最小调仓阈值时维持原仓位。"
            ),
            (
                "- 策略收益按目标仓位乘以策略代理标的下一交易日收益，"
                "并扣除单边交易成本和可配置线性滑点。"
            ),
            limitation_note,
            "- 当前版本未计入税费、汇率、融资利率、非线性盘口冲击、容量约束和盘中执行偏差。",
        ]
    )
    if sec_companyfacts_validation_report_path is not None:
        lines.append(f"- SEC companyfacts 校验报告：`{sec_companyfacts_validation_report_path}`")

    return "\n".join(lines) + "\n"


def write_backtest_report(
    result: DailyBacktestResult,
    data_quality_report_path: Path,
    daily_output_path: Path,
    output_path: Path,
    sec_companyfacts_validation_report_path: Path | None = None,
    input_coverage_output_path: Path | None = None,
    audit_report_path: Path | None = None,
    traceability_section: str | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_backtest_report(
            result,
            data_quality_report_path=data_quality_report_path,
            daily_output_path=daily_output_path,
            sec_companyfacts_validation_report_path=sec_companyfacts_validation_report_path,
            input_coverage_output_path=input_coverage_output_path,
            audit_report_path=audit_report_path,
            traceability_section=traceability_section,
        ),
        encoding="utf-8",
    )
    return output_path


def default_backtest_report_path(output_dir: Path, start: date, end: date) -> Path:
    return output_dir / f"backtest_{start.isoformat()}_{end.isoformat()}.md"


def default_backtest_daily_path(output_dir: Path, start: date, end: date) -> Path:
    return output_dir / f"backtest_daily_{start.isoformat()}_{end.isoformat()}.csv"


def default_backtest_input_coverage_path(
    output_dir: Path,
    start: date,
    end: date,
) -> Path:
    return output_dir / f"backtest_input_coverage_{start.isoformat()}_{end.isoformat()}.csv"


def _component_input_coverage_records(
    result: DailyBacktestResult,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in result.rows:
        month = row.signal_date.strftime("%Y-%m")
        for component, coverage in sorted(row.component_coverages.items()):
            records.append(
                _input_coverage_record(
                    "component_coverage",
                    month=month,
                    signal_date=row.signal_date.isoformat(),
                    input="评分模块",
                    component=component,
                    source_type=row.component_source_types.get(component, ""),
                    metric="coverage",
                    value=coverage,
                )
            )
    return records


def _input_issue_coverage_records(
    result: DailyBacktestResult,
) -> list[dict[str, object]]:
    if not result.monthly_input_issue_counts:
        return []

    return [
        _input_coverage_record(
            "input_issue",
            month=month,
            input=input_label,
            issue_code=code,
            subject=subject,
            count=count,
        )
        for (month, input_label, code, subject), count in sorted(
            result.monthly_input_issue_counts.items()
        )
    ]


def _input_source_url_coverage_records(
    result: DailyBacktestResult,
) -> list[dict[str, object]]:
    if not result.monthly_input_source_url_counts:
        return []

    return [
        _input_coverage_record(
            "input_source_url",
            month=month,
            input=input_label,
            source_type=source_type,
            subject=subject,
            source_url=source_url,
            count=count,
        )
        for (month, input_label, source_type, subject, source_url), count in sorted(
            result.monthly_input_source_url_counts.items()
        )
    ]


def _ticker_input_coverage_records(
    result: DailyBacktestResult,
) -> list[dict[str, object]]:
    if not result.monthly_ticker_input_counts:
        return []

    return [
        _input_coverage_record(
            "ticker_input",
            month=month,
            ticker=ticker,
            metric=metric,
            count=count,
        )
        for (month, ticker, metric), count in sorted(
            result.monthly_ticker_input_counts.items()
        )
    ]


def _ticker_feature_coverage_records(
    result: DailyBacktestResult,
) -> list[dict[str, object]]:
    if not result.monthly_ticker_feature_counts:
        return []

    return [
        _input_coverage_record(
            "ticker_sec_feature",
            month=month,
            ticker=ticker,
            feature_id=feature_id,
            period_type=period_type,
            count=count,
        )
        for (month, ticker, feature_id, period_type), count in sorted(
            result.monthly_ticker_feature_counts.items()
        )
    ]


def _risk_event_evidence_url_coverage_records(
    result: DailyBacktestResult,
) -> list[dict[str, object]]:
    if not result.monthly_risk_event_evidence_url_counts:
        return []

    return [
        _input_coverage_record(
            "risk_event_evidence_url",
            month=month,
            input="风险事件发生记录",
            event_id=event_id,
            occurrence_id=occurrence_id,
            status=status,
            score_eligible=score_state == "score_eligible",
            related_tickers=related_tickers,
            source_type=source_type,
            source_url=source_url,
            count=count,
        )
        for (
            month,
            event_id,
            occurrence_id,
            status,
            score_state,
            related_tickers,
            source_type,
            source_url,
        ), count in sorted(result.monthly_risk_event_evidence_url_counts.items())
    ]


def _input_source_type_coverage_records(
    result: DailyBacktestResult,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    if result.monthly_valuation_source_type_counts:
        records.extend(
            _input_coverage_record(
                "valuation_source_type",
                month=month,
                input="估值快照",
                source_type=source_type,
                count=count,
            )
            for (month, source_type), count in sorted(
                result.monthly_valuation_source_type_counts.items()
            )
        )
    if result.monthly_risk_event_source_type_counts:
        records.extend(
            _input_coverage_record(
                "risk_event_source_type",
                month=month,
                input="风险事件发生记录",
                source_type=source_type,
                count=count,
            )
            for (month, source_type), count in sorted(
                result.monthly_risk_event_source_type_counts.items()
            )
        )
    return records


def _input_coverage_record(
    record_type: str,
    **updates: object,
) -> dict[str, object]:
    record: dict[str, object] = {
        column: "" for column in BACKTEST_INPUT_COVERAGE_COLUMNS
    }
    record["record_type"] = record_type
    for key, value in updates.items():
        if key in record:
            record[key] = value
    return record


def _prepare_adjusted_close_pivot(prices: pd.DataFrame) -> pd.DataFrame:
    required_columns = {"date", "ticker", "adj_close"}
    missing = sorted(required_columns - set(prices.columns))
    if missing:
        raise ValueError(f"价格数据缺少必需字段：{', '.join(missing)}")

    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].copy()
    pivot = frame.pivot(index="_date", columns="ticker", values="_adj_close").sort_index()
    return pivot


def _check_required_tickers(close_pivot: pd.DataFrame, tickers: tuple[str, ...]) -> None:
    missing = [ticker for ticker in dict.fromkeys(tickers) if ticker not in close_pivot.columns]
    if missing:
        raise ValueError(f"回测缺少价格标的：{', '.join(missing)}")


def _backtest_periods(series: pd.Series, start: date, end: date) -> list[tuple[date, date]]:
    history = series.dropna().sort_index()
    timestamps = list(history.index)
    periods: list[tuple[date, date]] = []
    for index in range(len(timestamps) - 1):
        signal_date = pd.Timestamp(timestamps[index]).date()
        return_date = pd.Timestamp(timestamps[index + 1]).date()
        if signal_date >= start and return_date <= end:
            periods.append((signal_date, return_date))
    return periods


def _period_return(
    close_pivot: pd.DataFrame,
    ticker: str,
    signal_date: date,
    return_date: date,
) -> float:
    start_value = float(cast(Any, close_pivot.at[pd.Timestamp(signal_date), ticker]))
    end_value = float(cast(Any, close_pivot.at[pd.Timestamp(return_date), ticker]))
    if start_value <= 0:
        raise ValueError(f"{ticker} 在 {signal_date.isoformat()} 的价格非正")
    return (end_value / start_value) - 1.0


def _benchmark_metrics(
    close_pivot: pd.DataFrame,
    ticker: str,
    periods: list[tuple[date, date]],
) -> BacktestMetrics:
    returns = [
        _period_return(close_pivot, ticker, signal_date, return_date)
        for signal_date, return_date in periods
    ]
    return summarize_long_only_backtest(
        strategy_returns=returns,
        exposures=[1.0 for _ in returns],
        turnovers=[0.0 for _ in returns],
    )


def _position_midpoint(min_position: float, max_position: float) -> float:
    return (min_position + max_position) / 2.0


def _benchmark_policy_status(result: DailyBacktestResult) -> str:
    if result.benchmark_policy_report is None:
        return "未连接"
    return result.benchmark_policy_report.status


def _metrics_row(label: str, metrics: BacktestMetrics) -> str:
    return (
        "| "
        f"{label} | "
        f"{metrics.total_return:.1%} | "
        f"{metrics.cagr:.1%} | "
        f"{metrics.max_drawdown:.1%} | "
        f"{_optional_float(metrics.sharpe)} | "
        f"{_optional_float(metrics.sortino)} | "
        f"{_optional_float(metrics.calmar)} | "
        f"{metrics.time_in_market:.1%} | "
        f"{metrics.turnover:.1f} |"
    )


def _execution_cost_summary_lines(result: DailyBacktestResult) -> list[str]:
    commission_cost = sum(row.commission_cost for row in result.rows)
    slippage_cost = sum(row.slippage_cost for row in result.rows)
    transaction_cost = sum(row.transaction_cost for row in result.rows)
    turnover = sum(row.turnover for row in result.rows)
    return [
        "",
        "## 执行成本摘要",
        "",
        "| 项目 | 值 |",
        "|---|---:|",
        f"| 累计换手 | {turnover:.1f} |",
        f"| 单边交易成本扣减 | {commission_cost:.2%} |",
        f"| 线性滑点扣减 | {slippage_cost:.2%} |",
        f"| 总执行成本扣减 | {transaction_cost:.2%} |",
    ]


def _position_gate_summary_rows(result: DailyBacktestResult) -> list[str]:
    gate_caps: dict[str, list[float]] = {}
    gate_trigger_counts: Counter[str] = Counter()
    for row in result.rows:
        for gate_id, cap in row.position_gate_caps.items():
            gate_caps.setdefault(gate_id, []).append(cap)
        for gate_id, triggered in row.position_gate_triggers.items():
            if triggered:
                gate_trigger_counts[gate_id] += 1

    return [
        (
            "| "
            f"{gate_id} | "
            f"{len(caps)} | "
            f"{gate_trigger_counts.get(gate_id, 0)} | "
            f"{min(caps):.0%} | "
            f"{(sum(caps) / len(caps)):.0%} |"
        )
        for gate_id, caps in sorted(gate_caps.items())
    ]


def _confidence_summary_rows(result: DailyBacktestResult) -> list[str]:
    grouped_rows: dict[str, list[BacktestDailyRow]] = {}
    for row in result.rows:
        grouped_rows.setdefault(row.confidence_level, []).append(row)

    rows: list[str] = []
    for level in ("high", "medium", "low"):
        level_rows = grouped_rows.get(level)
        if not level_rows:
            continue
        average_score = sum(row.total_score for row in level_rows) / len(level_rows)
        average_position = sum(row.target_exposure for row in level_rows) / len(level_rows)
        average_return = sum(row.strategy_return for row in level_rows) / len(level_rows)
        rows.append(
            "| "
            f"{_confidence_level_label(level)}（{level}） | "
            f"{len(level_rows)} | "
            f"{average_score:.1f} | "
            f"{average_position:.0%} | "
            f"{average_return:.2%} |"
        )
    return rows


def _data_quality_gate_summary_lines(report: DataQualityReport) -> list[str]:
    lines = [
        "",
        "## 数据质量门禁摘要",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 错误数：{report.error_count}；警告数：{report.warning_count}",
        f"- 预期价格标的数：{len(report.expected_price_tickers)}",
        f"- 预期利率序列数：{len(report.expected_rate_series)}",
        "",
        "| 文件 | 路径 | 行数 | 日期范围 | Sha256 |",
        "|---|---|---:|---|---|",
        _data_quality_file_row("价格数据", report.price_summary),
        _data_quality_file_row("利率数据", report.rate_summary),
    ]
    if report.manifest_summary is not None:
        lines.append(_data_quality_file_row("下载审计清单", report.manifest_summary))

    lines.extend(["", "### 数据质量问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
        return lines

    lines.extend(
        [
            "| 级别 | Code | 行数 | 说明 | 样例 |",
            "|---|---|---:|---|---|",
        ]
    )
    for issue in report.issues:
        lines.append(
            "| "
            f"{_data_quality_severity_label(issue.severity)} | "
            f"{issue.code} | "
            f"{issue.rows if issue.rows is not None else ''} | "
            f"{_escape_markdown_table(issue.message)} | "
            f"{_escape_markdown_table(issue.sample or '')} |"
        )
    return lines


def _data_quality_file_row(label: str, summary: DataFileSummary) -> str:
    if not summary.exists:
        return (
            "| "
            f"{label} | "
            f"`{_escape_markdown_table(str(summary.path))}` | "
            "0 | n/a | n/a |"
        )

    min_date = summary.min_date.isoformat() if summary.min_date else "n/a"
    max_date = summary.max_date.isoformat() if summary.max_date else "n/a"
    checksum = summary.sha256 or "n/a"
    return (
        "| "
        f"{label} | "
        f"`{_escape_markdown_table(str(summary.path))}` | "
        f"{summary.rows} | "
        f"{min_date} 至 {max_date} | "
        f"{checksum} |"
    )


def _data_quality_severity_label(severity: object) -> str:
    if str(severity) == "ERROR":
        return "错误"
    if str(severity) == "WARNING":
        return "警告"
    return str(severity)


def _component_coverage_summary_rows(result: DailyBacktestResult) -> list[str]:
    coverage_values: dict[str, list[float]] = {}
    for row in result.rows:
        for component, coverage in row.component_coverages.items():
            coverage_values.setdefault(component, []).append(coverage)

    return [
        (
            "| "
            f"{_component_label(component)} | "
            f"{len(values)} | "
            f"{min(values):.0%} | "
            f"{(sum(values) / len(values)):.0%} | "
            f"{max(values):.0%} |"
        )
        for component, values in coverage_values.items()
    ]


def _monthly_component_coverage_trend_rows(result: DailyBacktestResult) -> list[str]:
    coverage_values: dict[tuple[str, str], list[float]] = {}
    for row in result.rows:
        month = row.signal_date.strftime("%Y-%m")
        for component, coverage in row.component_coverages.items():
            coverage_values.setdefault((month, component), []).append(coverage)

    rows: list[str] = []
    for (month, component), values in sorted(coverage_values.items()):
        insufficient_days = sum(1 for value in values if value < 1.0)
        rows.append(
            "| "
            f"{month} | "
            f"{_component_label(component)} | "
            f"{len(values)} | "
            f"{(sum(values) / len(values)):.0%} | "
            f"{min(values):.0%} | "
            f"{insufficient_days} |"
        )
    return rows


def _monthly_component_source_type_rows(result: DailyBacktestResult) -> list[str]:
    source_type_counts: dict[tuple[str, str], Counter[str]] = {}
    for row in result.rows:
        month = row.signal_date.strftime("%Y-%m")
        for component, source_type in row.component_source_types.items():
            source_type_counts.setdefault((month, component), Counter())[source_type] += 1

    rows: list[str] = []
    for (month, component), counts in sorted(source_type_counts.items()):
        sample_count = sum(counts.values())
        explanation_required_days = sum(
            count
            for source_type, count in counts.items()
            if source_type in _SOURCE_TYPES_REQUIRING_EXPLANATION
        )
        rows.append(
            "| "
            f"{month} | "
            f"{_component_label(component)} | "
            f"{sample_count} | "
            f"{_source_type_counts_summary(counts)} | "
            f"{explanation_required_days} |"
        )
    return rows


def _monthly_input_issue_rows(result: DailyBacktestResult) -> list[str]:
    if not result.monthly_input_issue_counts:
        return []

    return [
        (
            "| "
            f"{month} | "
            f"{input_label} | "
            f"{code} | "
            f"{_escape_markdown_table(subject)} | "
            f"{count} |"
        )
        for (month, input_label, code, subject), count in sorted(
            result.monthly_input_issue_counts.items()
        )
    ]


def _monthly_risk_event_source_type_rows(result: DailyBacktestResult) -> list[str]:
    if not result.monthly_risk_event_source_type_counts:
        return []

    return [
        (
            "| "
            f"{month} | "
            f"{_risk_event_source_type_label(source_type)} | "
            f"{count} |"
        )
        for (month, source_type), count in sorted(
            result.monthly_risk_event_source_type_counts.items()
        )
    ]


def _monthly_input_source_url_rows(result: DailyBacktestResult) -> list[str]:
    if not result.monthly_input_source_url_counts:
        return []

    return [
        (
            "| "
            f"{month} | "
            f"{input_label} | "
            f"{_input_source_type_label(input_label, source_type)} | "
            f"{_escape_markdown_table(subject)} | "
            f"{_escape_markdown_table(source_url)} | "
            f"{count} |"
        )
        for (month, input_label, source_type, subject, source_url), count in sorted(
            result.monthly_input_source_url_counts.items()
        )
    ]


def _monthly_risk_event_evidence_url_rows(result: DailyBacktestResult) -> list[str]:
    if not result.monthly_risk_event_evidence_url_counts:
        return []

    return [
        (
            "| "
            f"{month} | "
            f"{_escape_markdown_table(event_id)} | "
            f"{_escape_markdown_table(occurrence_id)} | "
            f"{_occurrence_status_label(status)} | "
            f"{_score_eligible_label(score_state)} | "
            f"{_escape_markdown_table(related_tickers)} | "
            f"{_risk_event_source_type_label(source_type)} | "
            f"{_escape_markdown_table(source_url)} | "
            f"{count} |"
        )
        for (
            month,
            event_id,
            occurrence_id,
            status,
            score_state,
            related_tickers,
            source_type,
            source_url,
        ), count in sorted(result.monthly_risk_event_evidence_url_counts.items())
    ]


def _monthly_ticker_input_rows(result: DailyBacktestResult) -> list[str]:
    if not result.monthly_ticker_input_counts:
        return []

    month_tickers = sorted(
        {
            (month, ticker)
            for month, ticker, _metric in result.monthly_ticker_input_counts
        }
    )
    rows: list[str] = []
    for month, ticker in month_tickers:
        rows.append(
            "| "
            f"{month} | "
            f"{ticker} | "
            f"{_monthly_ticker_metric_count(result, month, ticker, 'sec_feature_rows')} | "
            f"{_monthly_ticker_metric_count(result, month, ticker, 'sec_missing_observations')} | "
            f"{_monthly_ticker_metric_count(result, month, ticker, 'valuation_snapshots')} | "
            f"{_monthly_ticker_metric_count(result, month, ticker, 'active_risk_events')} | "
            f"{_monthly_ticker_metric_count(result, month, ticker, 'score_eligible_risk_events')} |"
        )
    return rows


def _monthly_ticker_feature_rows(result: DailyBacktestResult) -> list[str]:
    if not result.monthly_ticker_feature_counts:
        return []

    return [
        (
            "| "
            f"{month} | "
            f"{ticker} | "
            f"{_escape_markdown_table(feature_id)} | "
            f"{period_type} | "
            f"{count} |"
        )
        for (month, ticker, feature_id, period_type), count in sorted(
            result.monthly_ticker_feature_counts.items()
        )
    ]


def _monthly_ticker_metric_count(
    result: DailyBacktestResult,
    month: str,
    ticker: str,
    metric: str,
) -> int:
    if not result.monthly_ticker_input_counts:
        return 0
    return result.monthly_ticker_input_counts.get((month, ticker, metric), 0)


def _monthly_valuation_source_type_rows(result: DailyBacktestResult) -> list[str]:
    if not result.monthly_valuation_source_type_counts:
        return []

    return [
        (
            "| "
            f"{month} | "
            f"{_valuation_source_type_label(source_type)} | "
            f"{count} |"
        )
        for (month, source_type), count in sorted(
            result.monthly_valuation_source_type_counts.items()
        )
    ]


def _record_monthly_input_issues(
    monthly_input_issue_counts: Counter[tuple[str, str, str, str]],
    signal_date: date,
    input_label: str,
    issues: tuple[object, ...],
) -> None:
    month = signal_date.strftime("%Y-%m")
    for issue in issues:
        code = _issue_attr(issue, "code") or "unknown_issue"
        subject = _issue_subject(issue)
        monthly_input_issue_counts[(month, input_label, code, subject)] += 1


def _record_monthly_valuation_source_urls(
    monthly_input_source_url_counts: Counter[tuple[str, str, str, str, str]],
    signal_date: date,
    report: ValuationReviewReport,
) -> None:
    month = signal_date.strftime("%Y-%m")
    for loaded in report.validation_report.snapshots:
        snapshot = loaded.snapshot
        source_url = snapshot.source_url.strip()
        if not source_url:
            continue
        subject = f"{snapshot.ticker}:{snapshot.snapshot_id}"
        monthly_input_source_url_counts[
            (
                month,
                "估值快照",
                snapshot.source_type,
                subject,
                source_url,
            )
        ] += 1


def _record_monthly_valuation_ticker_inputs(
    monthly_ticker_input_counts: Counter[tuple[str, str, str]],
    signal_date: date,
    report: ValuationReviewReport,
) -> None:
    month = signal_date.strftime("%Y-%m")
    for loaded in report.validation_report.snapshots:
        ticker = loaded.snapshot.ticker
        monthly_ticker_input_counts[(month, ticker, "valuation_snapshots")] += 1


def _record_monthly_valuation_source_types(
    monthly_valuation_source_type_counts: Counter[tuple[str, str]],
    signal_date: date,
    report: ValuationReviewReport,
) -> None:
    month = signal_date.strftime("%Y-%m")
    for loaded in report.validation_report.snapshots:
        source_type = loaded.snapshot.source_type
        monthly_valuation_source_type_counts[(month, source_type)] += 1


def _record_monthly_risk_event_source_urls(
    monthly_input_source_url_counts: Counter[tuple[str, str, str, str, str]],
    signal_date: date,
    report: RiskEventOccurrenceReviewReport,
) -> None:
    month = signal_date.strftime("%Y-%m")
    for loaded in report.validation_report.occurrences:
        occurrence = loaded.occurrence
        subject = f"{occurrence.event_id}:{occurrence.occurrence_id}"
        for source in occurrence.evidence_sources:
            source_url = source.source_url.strip()
            if not source_url:
                continue
            monthly_input_source_url_counts[
                (
                    month,
                    "风险事件发生记录",
                    source.source_type,
                    subject,
                    source_url,
                )
            ] += 1


def _record_monthly_risk_event_evidence_urls(
    monthly_risk_event_evidence_url_counts: Counter[
        tuple[str, str, str, str, str, str, str, str]
    ],
    signal_date: date,
    report: RiskEventOccurrenceReviewReport,
) -> None:
    month = signal_date.strftime("%Y-%m")
    items_by_occurrence_id = {item.occurrence_id: item for item in report.items}
    rules_by_id = {rule.event_id: rule for rule in report.validation_report.config.event_rules}
    for loaded in report.validation_report.occurrences:
        occurrence = loaded.occurrence
        item = items_by_occurrence_id.get(occurrence.occurrence_id)
        score_state = (
            "score_eligible"
            if item is not None and item.score_eligible
            else "not_score_eligible"
        )
        rule = rules_by_id.get(occurrence.event_id)
        related_tickers = ", ".join(rule.related_tickers if rule is not None else [])
        for source in occurrence.evidence_sources:
            source_url = source.source_url.strip()
            if not source_url:
                continue
            monthly_risk_event_evidence_url_counts[
                (
                    month,
                    occurrence.event_id,
                    occurrence.occurrence_id,
                    occurrence.status,
                    score_state,
                    related_tickers,
                    source.source_type,
                    source_url,
                )
            ] += 1


def _record_monthly_risk_event_ticker_inputs(
    monthly_ticker_input_counts: Counter[tuple[str, str, str]],
    signal_date: date,
    report: RiskEventOccurrenceReviewReport,
) -> None:
    month = signal_date.strftime("%Y-%m")
    rules_by_id = {rule.event_id: rule for rule in report.validation_report.config.event_rules}
    for item in report.active_items:
        rule = rules_by_id.get(item.event_id)
        if rule is None:
            continue
        for ticker in rule.related_tickers:
            monthly_ticker_input_counts[(month, ticker, "active_risk_events")] += 1
            if item.score_eligible:
                monthly_ticker_input_counts[
                    (month, ticker, "score_eligible_risk_events")
                ] += 1


def _record_monthly_risk_event_source_types(
    monthly_risk_event_source_type_counts: Counter[tuple[str, str]],
    signal_date: date,
    report: RiskEventOccurrenceReviewReport,
) -> None:
    month = signal_date.strftime("%Y-%m")
    for item in report.items:
        for source_type in item.source_types:
            monthly_risk_event_source_type_counts[(month, source_type)] += 1


def _record_monthly_sec_feature_inputs(
    monthly_ticker_input_counts: Counter[tuple[str, str, str]],
    monthly_ticker_feature_counts: Counter[tuple[str, str, str, str]],
    signal_date: date,
    report: SecFundamentalFeaturesReport,
) -> None:
    month = signal_date.strftime("%Y-%m")
    for row in report.rows:
        monthly_ticker_input_counts[(month, row.ticker, "sec_feature_rows")] += 1
        monthly_ticker_feature_counts[
            (month, row.ticker, row.feature_id, row.period_type)
        ] += 1


def _record_monthly_sec_missing_observations(
    monthly_input_issue_counts: Counter[tuple[str, str, str, str]],
    monthly_ticker_input_counts: Counter[tuple[str, str, str]],
    signal_date: date,
    missing_observation_keys: tuple[tuple[str, str, str], ...],
) -> None:
    month = signal_date.strftime("%Y-%m")
    for ticker, metric_id, period_type in missing_observation_keys:
        monthly_ticker_input_counts[(month, ticker, "sec_missing_observations")] += 1
        subject = f"{ticker}:{metric_id}:{period_type}"
        monthly_input_issue_counts[
            (
                month,
                "SEC 基本面",
                "sec_fundamental_metrics_missing_observation",
                subject,
            )
        ] += 1


def _issue_subject(issue: object) -> str:
    attrs = (
        "ticker",
        "metric_id",
        "feature_id",
        "period_type",
        "snapshot_id",
        "event_id",
        "level",
    )
    parts = [
        value
        for attr in attrs
        if (value := _issue_attr(issue, attr)) is not None
    ]
    return ":".join(parts) if parts else "global"


def _issue_attr(issue: object, attr: str) -> str | None:
    value = cast(object | None, getattr(issue, attr, None))
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _source_type_counts_summary(counts: Counter[str]) -> str:
    ordered_source_types = sorted(
        counts,
        key=lambda source_type: (
            _SOURCE_TYPE_ORDER.index(source_type)
            if source_type in _SOURCE_TYPE_ORDER
            else len(_SOURCE_TYPE_ORDER),
            source_type,
        ),
    )
    return "；".join(
        f"{_source_type_label(source_type)} {counts[source_type]}"
        for source_type in ordered_source_types
    )


def _component_label(name: str) -> str:
    label = COMPONENT_LABELS.get(name)
    if label is None:
        return name
    return f"{label}（{name}）"


def _source_type_label(source_type: str) -> str:
    return SOURCE_TYPE_LABELS.get(source_type, source_type)


def _risk_event_source_type_label(source_type: str) -> str:
    label = _RISK_EVENT_SOURCE_TYPE_LABELS.get(source_type, source_type)
    return f"{label}（{source_type}）"


def _valuation_source_type_label(source_type: str) -> str:
    label = _VALUATION_SOURCE_TYPE_LABELS.get(source_type, source_type)
    return f"{label}（{source_type}）"


def _input_source_type_label(input_label: str, source_type: str) -> str:
    if input_label == "估值快照":
        return _valuation_source_type_label(source_type)
    if input_label == "风险事件发生记录":
        return _risk_event_source_type_label(source_type)
    return source_type


def _occurrence_status_label(status: str) -> str:
    labels = {
        "active": "活跃",
        "watch": "观察",
        "resolved": "已解除",
        "dismissed": "已排除",
    }
    label = labels.get(status, status)
    return f"{label}（{status}）"


def _score_eligible_label(score_state: str) -> str:
    if score_state == "score_eligible":
        return "是"
    if score_state == "not_score_eligible":
        return "否"
    return score_state


def _confidence_level_label(level: str) -> str:
    return {
        "high": "高",
        "medium": "中",
        "low": "低",
    }.get(level, level)


def _escape_markdown_table(text: str) -> str:
    return text.replace("\n", " ").replace("|", "\\|")


def _backtest_limitation_note(result: DailyBacktestResult) -> str:
    included: list[str] = []
    missing: list[str] = []
    if result.fundamental_feature_report_count:
        included.append("SEC 基本面特征")
    else:
        missing.append("SEC 基本面特征")
    if result.valuation_review_report_count:
        included.append("估值快照")
    else:
        missing.append("估值快照")
    if result.risk_event_occurrence_review_report_count:
        included.append("政策/地缘风险事件发生记录")
    else:
        missing.append("政策/地缘风险事件发生记录")

    if missing:
        prefix = (
            f"已按 signal_date point-in-time 接入：{'、'.join(included)}；"
            if included
            else "尚未接入 point-in-time 历史输入；"
        )
        return (
            f"- {prefix}尚未接入：{'、'.join(missing)}，"
            "因此回测状态标记为 PASS_WITH_LIMITATIONS。"
        )
    return (
        "- SEC 基本面、估值快照和政策/地缘风险事件发生记录已按 "
        "signal_date point-in-time 接入；回测状态仍因税费、汇率、融资、容量约束"
        "和非线性执行冲击等简化假设标记为 PASS_WITH_LIMITATIONS。"
    )


def _status_counts_summary(status_counts: dict[str, int] | None) -> str:
    if not status_counts:
        return "无"
    return ", ".join(f"{status}={count}" for status, count in sorted(status_counts.items()))


def _count_range(min_count: int | None, max_count: int | None) -> str:
    if min_count is None or max_count is None:
        return "n/a"
    if min_count == max_count:
        return str(min_count)
    return f"{min_count}-{max_count}"


def _optional_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"
