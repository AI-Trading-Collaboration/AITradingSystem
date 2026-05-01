from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import pandas as pd

from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.config import FeatureConfig, PortfolioConfig, ScoringRulesConfig
from ai_trading_system.data.quality import DataQualityReport
from ai_trading_system.features.market import build_market_features
from ai_trading_system.scoring.daily import build_daily_score_report

DEFAULT_BENCHMARK_TICKERS = ("SPY", "QQQ", "SMH", "SOXX")


@dataclass(frozen=True)
class BacktestDailyRow:
    signal_date: date
    return_date: date
    total_score: float
    position_label: str
    raw_target_exposure: float
    target_exposure: float
    asset_return: float
    gross_return: float
    turnover: float
    transaction_cost: float
    strategy_return: float
    strategy_equity: float
    component_scores: dict[str, float]

    def to_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "signal_date": self.signal_date.isoformat(),
            "return_date": self.return_date.isoformat(),
            "total_score": self.total_score,
            "position_label": self.position_label,
            "raw_target_exposure": self.raw_target_exposure,
            "target_exposure": self.target_exposure,
            "asset_return": self.asset_return,
            "gross_return": self.gross_return,
            "turnover": self.turnover,
            "transaction_cost": self.transaction_cost,
            "strategy_return": self.strategy_return,
            "strategy_equity": self.strategy_equity,
        }
        for component, score in self.component_scores.items():
            record[f"{component}_score"] = score
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
    minimum_action_delta: float
    data_quality_report: DataQualityReport
    rows: tuple[BacktestDailyRow, ...]
    strategy_metrics: BacktestMetrics
    benchmark_metrics: dict[str, BacktestMetrics]

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
) -> DailyBacktestResult:
    if start >= end:
        raise ValueError("回测开始日期必须早于结束日期")
    if cost_bps < 0:
        raise ValueError("交易成本 bps 不能为负数")

    close_pivot = _prepare_adjusted_close_pivot(prices)
    _check_required_tickers(close_pivot, (strategy_ticker, *benchmark_tickers))
    periods = _backtest_periods(close_pivot[strategy_ticker], start, end)
    if not periods:
        raise ValueError("回测区间内没有可用的下一交易日收益")

    cost_rate = cost_bps / 10_000.0
    previous_exposure = 0.0
    running_equity = 1.0
    rows: list[BacktestDailyRow] = []

    for signal_date, return_date in periods:
        feature_set = build_market_features(
            prices=prices,
            rates=rates,
            config=feature_config,
            as_of=signal_date,
            core_watchlist=core_watchlist,
        )
        score_report = build_daily_score_report(
            feature_set=feature_set,
            data_quality_report=data_quality_report,
            rules=scoring_rules,
            total_risk_asset_min=portfolio_config.portfolio.total_risk_asset_min,
            total_risk_asset_max=portfolio_config.portfolio.total_risk_asset_max,
        )
        recommendation = score_report.recommendation
        raw_target_exposure = _position_midpoint(
            recommendation.risk_asset_ai_band.min_position,
            recommendation.risk_asset_ai_band.max_position,
        )
        if not rows or abs(raw_target_exposure - previous_exposure) >= (
            scoring_rules.position_change.minimum_action_delta
        ):
            target_exposure = raw_target_exposure
        else:
            target_exposure = previous_exposure

        asset_return = _period_return(close_pivot, strategy_ticker, signal_date, return_date)
        gross_return = target_exposure * asset_return
        turnover = abs(target_exposure - previous_exposure)
        transaction_cost = turnover * cost_rate
        strategy_return = gross_return - transaction_cost
        running_equity *= 1.0 + strategy_return

        rows.append(
            BacktestDailyRow(
                signal_date=signal_date,
                return_date=return_date,
                total_score=recommendation.total_score,
                position_label=recommendation.label,
                raw_target_exposure=raw_target_exposure,
                target_exposure=target_exposure,
                asset_return=asset_return,
                gross_return=gross_return,
                turnover=turnover,
                transaction_cost=transaction_cost,
                strategy_return=strategy_return,
                strategy_equity=running_equity,
                component_scores={
                    component.name: component.score for component in score_report.components
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
        minimum_action_delta=scoring_rules.position_change.minimum_action_delta,
        data_quality_report=data_quality_report,
        rows=tuple(rows),
        strategy_metrics=strategy_metrics,
        benchmark_metrics=benchmark_metrics,
    )


def write_backtest_daily_csv(result: DailyBacktestResult, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row.to_record() for row in result.rows]).to_csv(output_path, index=False)
    return output_path


def render_backtest_report(
    result: DailyBacktestResult,
    data_quality_report_path: Path,
    daily_output_path: Path,
) -> str:
    lines = [
        "# 历史回测报告",
        "",
        f"- 状态：{result.status}",
        f"- 请求区间：{result.requested_start.isoformat()} 至 {result.requested_end.isoformat()}",
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
        f"- 单边交易成本：{result.cost_bps:.1f} bps",
        f"- 最小调仓阈值：{result.minimum_action_delta:.0%}",
        f"- 数据质量状态：{result.data_quality_report.status}",
        f"- 数据质量报告：`{data_quality_report_path}`",
        f"- 每日回测明细：`{daily_output_path}`",
        "",
        "## 核心指标",
        "",
        "| 组合 | 总收益 | CAGR | 最大回撤 | Sharpe | Sortino | Calmar | 在场比例 | 换手 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        _metrics_row(f"策略（{result.strategy_ticker} 动态仓位）", result.strategy_metrics),
    ]

    for ticker in result.benchmark_tickers:
        lines.append(_metrics_row(f"基准（{ticker} 买入持有）", result.benchmark_metrics[ticker]))

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 每个交易日收盘后计算评分，目标仓位从下一交易日收益开始生效，避免未来函数。",
            "- 目标仓位使用 AI 仓位区间中点；变化小于最小调仓阈值时维持原仓位。",
            "- 策略收益按目标仓位乘以策略代理标的下一交易日收益，并扣除单边换手成本。",
            (
                "- 基本面、估值、政策/地缘模块当前仍是 MVP 占位输入，"
                "因此回测状态标记为 PASS_WITH_LIMITATIONS。"
            ),
            "- 当前版本未计入税费、汇率、融资利率、盘口冲击和盘中执行偏差。",
        ]
    )

    return "\n".join(lines) + "\n"


def write_backtest_report(
    result: DailyBacktestResult,
    data_quality_report_path: Path,
    daily_output_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_backtest_report(
            result,
            data_quality_report_path=data_quality_report_path,
            daily_output_path=daily_output_path,
        ),
        encoding="utf-8",
    )
    return output_path


def default_backtest_report_path(output_dir: Path, start: date, end: date) -> Path:
    return output_dir / f"backtest_{start.isoformat()}_{end.isoformat()}.md"


def default_backtest_daily_path(output_dir: Path, start: date, end: date) -> Path:
    return output_dir / f"backtest_daily_{start.isoformat()}_{end.isoformat()}.csv"


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


def _optional_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"
