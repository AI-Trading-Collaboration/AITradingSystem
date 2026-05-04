from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ai_trading_system.backtest.daily import DailyBacktestResult
from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest


@dataclass(frozen=True)
class BacktestRobustnessScenario:
    scenario_id: str
    label: str
    category: str
    description: str
    result: DailyBacktestResult | None = None
    metrics: BacktestMetrics | None = None
    first_signal_date: date | None = None
    last_signal_date: date | None = None
    skipped_reason: str | None = None

    @property
    def status(self) -> str:
        if self.skipped_reason:
            return "SKIPPED"
        if self.result is None and self.metrics is None:
            return "NOT_RUN"
        if self.result is not None:
            return self.result.status
        return "PASS_WITH_LIMITATIONS"


@dataclass(frozen=True)
class BacktestRobustnessReport:
    base_result: DailyBacktestResult
    scenarios: tuple[BacktestRobustnessScenario, ...]
    cost_stress_increment_bps: float
    shifted_start_days: int

    @property
    def status(self) -> str:
        completed = [
            scenario
            for scenario in self.scenarios
            if scenario.result is not None or scenario.metrics is not None
        ]
        if not completed:
            return "INSUFFICIENT_SCENARIOS"
        return "PASS_WITH_LIMITATIONS"


def default_backtest_robustness_report_path(
    output_dir: Path,
    start: date,
    end: date,
) -> Path:
    return output_dir / f"backtest_robustness_{start.isoformat()}_{end.isoformat()}.md"


def default_backtest_robustness_summary_path(
    output_dir: Path,
    start: date,
    end: date,
) -> Path:
    return output_dir / f"backtest_robustness_{start.isoformat()}_{end.isoformat()}.json"


def write_backtest_robustness_report(
    report: BacktestRobustnessReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_backtest_robustness_report(report), encoding="utf-8")
    return output_path


def write_backtest_robustness_summary(
    report: BacktestRobustnessReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            backtest_robustness_summary_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return output_path


def fixed_total_asset_exposure_scenario(
    result: DailyBacktestResult,
    *,
    exposure: float = 0.60,
) -> BacktestRobustnessScenario:
    metrics = _fixed_exposure_metrics(result, exposure=exposure)
    return BacktestRobustnessScenario(
        scenario_id=f"fixed_{int(exposure * 100)}pct_total_asset_ai",
        label=f"固定 {exposure:.0%} 总资产 AI exposure",
        category="baseline",
        description=(
            f"每天使用固定 {exposure:.0%} 总资产 AI exposure，复用基础回测的"
            "下一交易日收益、显式成本假设和信号日期；不读取额外数据。"
        ),
        metrics=metrics,
        first_signal_date=result.first_signal_date,
        last_signal_date=result.last_signal_date,
    )


def backtest_robustness_summary_record(
    report: BacktestRobustnessReport,
) -> dict[str, object]:
    result = report.base_result
    return {
        "schema_version": 1,
        "report_type": "backtest_robustness",
        "production_effect": "none",
        "status": report.status,
        "requested_start": result.requested_start.isoformat(),
        "requested_end": result.requested_end.isoformat(),
        "first_signal_date": result.first_signal_date.isoformat(),
        "last_signal_date": result.last_signal_date.isoformat(),
        "market_regime": (
            None
            if result.market_regime is None
            else {
                "regime_id": result.market_regime.regime_id,
                "name": result.market_regime.name,
                "start_date": result.market_regime.start_date.isoformat(),
                "anchor_date": result.market_regime.anchor_date.isoformat(),
                "anchor_event": result.market_regime.anchor_event,
            }
        ),
        "data_quality_status": result.data_quality_report.status,
        "cost_stress_increment_bps": report.cost_stress_increment_bps,
        "shifted_start_days": report.shifted_start_days,
        "base_dynamic": _summary_metrics_record(result.strategy_metrics),
        "scenarios": [
            _scenario_summary_record(scenario, result.strategy_metrics)
            for scenario in report.scenarios
        ],
        "benchmarks": [
            {
                "benchmark": ticker,
                **_summary_metrics_record(metrics),
                "total_return_delta_vs_base": (
                    metrics.total_return - result.strategy_metrics.total_return
                ),
            }
            for ticker, metrics in sorted(result.benchmark_metrics.items())
        ],
        "remaining_gaps": (
            "module_weight_perturbation",
            "trend_only_baseline",
            "trend_plus_risk_sentiment_baseline",
            "same_turnover_random_strategy",
            "out_of_sample_validation",
        ),
    }


def render_backtest_robustness_report(report: BacktestRobustnessReport) -> str:
    result = report.base_result
    lines = [
        "# 回测稳健性报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect=none",
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
    lines.extend(
        [
            f"- 实际基础信号区间：{result.first_signal_date.isoformat()} 至 "
            f"{result.last_signal_date.isoformat()}",
            f"- 数据质量状态：{result.data_quality_report.status}",
            f"- 成本压力增量：{report.cost_stress_increment_bps:.1f} bps",
            f"- 起点后移天数：{report.shifted_start_days}",
            "",
            "## 方法边界",
            "",
            (
                "第二阶段复用同一批 point-in-time 输入运行成本压力、起点后移、"
                "固定总资产 AI exposure 和买入持有基准对比；它不是完整的防过拟合证明。"
            ),
            (
                "成本压力实验只改变显式成本假设，不改变价格、基本面、估值、风险事件、"
                "观察池 lifecycle 或评分规则输入。"
            ),
            (
                "本报告不改变 production scoring、position_gate、日报结论或任何执行建议；"
                "完整生产信任仍需要历史估值/风险事件覆盖、权重扰动、随机基线、"
                "再平衡频率和样本外验证。"
            ),
            "",
            "## 稳健性实验",
            "",
            (
                "| 实验 | 状态 | 信号区间 | 总收益 | 最大回撤 | Sharpe | 换手 | "
                "相对基础收益 | 说明 |"
            ),
            "|---|---|---|---:|---:|---:|---:|---:|---|",
            _result_row(
                label="base_dynamic",
                status=result.status,
                metrics=result.strategy_metrics,
                first_signal_date=result.first_signal_date,
                last_signal_date=result.last_signal_date,
                base_metrics=result.strategy_metrics,
                note="现有动态仓位规则，使用基础成本假设。",
            ),
        ]
    )
    for scenario in report.scenarios:
        lines.append(_scenario_row(scenario, result.strategy_metrics))

    lines.extend(
        [
            "",
            "## 买入持有基准",
            "",
            "| 基准 | 总收益 | 最大回撤 | Sharpe | 相对动态策略收益 | 解释用途 |",
            "|---|---:|---:|---:|---:|---|",
        ]
    )
    for ticker, metrics in result.benchmark_metrics.items():
        lines.append(_benchmark_row(ticker, metrics, result.strategy_metrics))

    lines.extend(
        [
            "",
            "## 初步解释",
            "",
            *_interpretation_lines(report),
            "",
            "## 剩余缺口",
            "",
            "- 尚未运行不同再平衡频率实验。",
            "- 尚未运行模块权重扰动、趋势-only 和趋势+风险情绪基线。",
            "- 尚未运行同换手率随机策略和显著性表达。",
            "- 历史估值快照和风险事件发生记录覆盖不足时，投资解释仍应降级。",
        ]
    )
    return "\n".join(lines) + "\n"


def _scenario_row(
    scenario: BacktestRobustnessScenario,
    base_metrics: BacktestMetrics,
) -> str:
    if scenario.result is None and scenario.metrics is None:
        note = scenario.skipped_reason or scenario.description
        return (
            f"| {scenario.scenario_id} | {scenario.status} | n/a | n/a | n/a | "
            f"n/a | n/a | n/a | {note} |"
        )
    if scenario.result is None:
        if scenario.metrics is None:
            raise ValueError("scenario metrics unexpectedly missing")
        if scenario.first_signal_date is None or scenario.last_signal_date is None:
            raise ValueError("scenario date window unexpectedly missing")
        return _result_row(
            label=scenario.scenario_id,
            status=scenario.status,
            metrics=scenario.metrics,
            first_signal_date=scenario.first_signal_date,
            last_signal_date=scenario.last_signal_date,
            base_metrics=base_metrics,
            note=scenario.description,
        )
    return _result_row(
        label=scenario.scenario_id,
        status=scenario.status,
        metrics=scenario.result.strategy_metrics,
        first_signal_date=scenario.result.first_signal_date,
        last_signal_date=scenario.result.last_signal_date,
        base_metrics=base_metrics,
        note=scenario.description,
    )


def _result_row(
    *,
    label: str,
    status: str,
    metrics: BacktestMetrics,
    first_signal_date: date,
    last_signal_date: date,
    base_metrics: BacktestMetrics,
    note: str,
) -> str:
    return (
        f"| {label} | {status} | {first_signal_date.isoformat()} 至 "
        f"{last_signal_date.isoformat()} | {_format_percent(metrics.total_return)} | "
        f"{_format_percent(metrics.max_drawdown)} | {_format_optional(metrics.sharpe)} | "
        f"{metrics.turnover:.1f} | "
        f"{_format_percent(metrics.total_return - base_metrics.total_return)} | {note} |"
    )


def _benchmark_row(
    ticker: str,
    metrics: BacktestMetrics,
    strategy_metrics: BacktestMetrics,
) -> str:
    return (
        f"| {ticker} buy-and-hold | {_format_percent(metrics.total_return)} | "
        f"{_format_percent(metrics.max_drawdown)} | {_format_optional(metrics.sharpe)} | "
        f"{_format_percent(metrics.total_return - strategy_metrics.total_return)} | "
        f"{_benchmark_role(ticker)} |"
    )


def _interpretation_lines(report: BacktestRobustnessReport) -> list[str]:
    result = report.base_result
    strategy = result.strategy_metrics
    benchmark_metrics = tuple(result.benchmark_metrics.values())
    lines: list[str] = []
    if benchmark_metrics:
        best_total_return = max(metrics.total_return for metrics in benchmark_metrics)
        best_drawdown = max(metrics.max_drawdown for metrics in benchmark_metrics)
        if strategy.total_return >= best_total_return:
            lines.append(
                "- 动态策略的总收益不低于本次可用买入持有基准，价值来源包含收益增强。"
            )
        else:
            lines.append(
                "- 动态策略未跑赢最高收益买入持有基准，不能把本次结果解释为纯收益增强。"
            )
        if strategy.max_drawdown >= best_drawdown:
            lines.append(
                "- 动态策略最大回撤不深于本次可用买入持有基准，回撤控制是可观察价值来源。"
            )
        else:
            lines.append(
                "- 动态策略最大回撤深于至少一个买入持有基准，回撤控制结论需要继续验证。"
            )
    if strategy.time_in_market < 0.95:
        lines.append(
            "- 动态策略不是长期满仓，仓位纪律和风险暴露管理应与收益指标分开评估。"
        )
    cost_scenario = _find_scenario(report, "cost_stress_execution")
    if cost_scenario is not None and cost_scenario.result is not None:
        delta = cost_scenario.result.strategy_metrics.total_return - strategy.total_return
        if delta < -0.02:
            lines.append(
                "- 成本压力对总收益造成明显拖累，生产前必须用真实成交样本校验成本假设。"
            )
        else:
            lines.append("- 成本压力实验未显著改变总收益方向，但仍不替代真实成交质量验证。")
    shifted_scenario = _find_scenario(report, "shifted_start")
    if shifted_scenario is not None and shifted_scenario.result is not None:
        delta = shifted_scenario.result.strategy_metrics.total_return - strategy.total_return
        if abs(delta) > 0.05:
            lines.append(
                "- 起点后移后结果变化较大，说明样本窗口敏感性仍是主要生产化风险。"
            )
        else:
            lines.append("- 起点后移后总收益变化有限，窗口敏感性在第一阶段实验中未明显恶化。")
    lines.append(
        "- 当前结论仍是研究和审计输入；完整生产信任需要剩余稳健性实验和 owner 审批。"
    )
    return lines


def _fixed_exposure_metrics(
    result: DailyBacktestResult,
    *,
    exposure: float,
) -> BacktestMetrics:
    if not 0 <= exposure <= 1:
        raise ValueError("fixed exposure must satisfy 0 <= exposure <= 1")
    commission_rate = result.cost_bps / 10_000.0
    spread_rate = result.spread_bps / 10_000.0
    slippage_rate = result.slippage_bps / 10_000.0
    market_impact_rate = result.market_impact_bps / 10_000.0
    tax_rate = result.tax_bps / 10_000.0
    fx_rate = result.fx_bps / 10_000.0
    financing_daily_rate = result.financing_annual_bps / 10_000.0 / 252.0
    etf_delay_rate = result.etf_delay_bps / 10_000.0

    previous_exposure = 0.0
    returns: list[float] = []
    exposures: list[float] = []
    turnovers: list[float] = []
    for row in result.rows:
        turnover = abs(exposure - previous_exposure)
        sell_turnover = max(previous_exposure - exposure, 0.0)
        transaction_cost = (
            turnover * commission_rate
            + turnover * spread_rate
            + turnover * slippage_rate
            + turnover * market_impact_rate
            + sell_turnover * tax_rate
            + turnover * fx_rate
            + exposure * financing_daily_rate
            + turnover * etf_delay_rate
        )
        returns.append(exposure * row.asset_return - transaction_cost)
        exposures.append(exposure)
        turnovers.append(turnover)
        previous_exposure = exposure
    return summarize_long_only_backtest(
        strategy_returns=returns,
        exposures=exposures,
        turnovers=turnovers,
    )


def _scenario_summary_record(
    scenario: BacktestRobustnessScenario,
    base_metrics: BacktestMetrics,
) -> dict[str, object]:
    metrics = (
        scenario.result.strategy_metrics
        if scenario.result is not None
        else scenario.metrics
    )
    record: dict[str, object] = {
        "scenario_id": scenario.scenario_id,
        "label": scenario.label,
        "category": scenario.category,
        "status": scenario.status,
        "description": scenario.description,
        "skipped_reason": scenario.skipped_reason,
    }
    if metrics is not None:
        record.update(_summary_metrics_record(metrics))
        record["total_return_delta_vs_base"] = (
            metrics.total_return - base_metrics.total_return
        )
    return record


def _summary_metrics_record(metrics: BacktestMetrics) -> dict[str, object]:
    return {
        "total_return": metrics.total_return,
        "cagr": metrics.cagr,
        "max_drawdown": metrics.max_drawdown,
        "sharpe": metrics.sharpe,
        "sortino": metrics.sortino,
        "calmar": metrics.calmar,
        "time_in_market": metrics.time_in_market,
        "turnover": metrics.turnover,
    }


def _find_scenario(
    report: BacktestRobustnessReport,
    scenario_id: str,
) -> BacktestRobustnessScenario | None:
    for scenario in report.scenarios:
        if scenario.scenario_id == scenario_id:
            return scenario
    return None


def _benchmark_role(ticker: str) -> str:
    roles = {
        "SMH": "半导体 beta 基线",
        "SOXX": "半导体 ETF 替代基线",
        "QQQ": "纳指成长 beta 基线",
        "SPY": "美股大盘 beta 基线",
    }
    return roles.get(ticker.upper(), "用户配置买入持有基线")


def _format_percent(value: float) -> str:
    return f"{value:.1%}"


def _format_optional(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"
