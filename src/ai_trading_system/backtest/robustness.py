from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from math import sqrt
from pathlib import Path
from random import Random
from statistics import pstdev

from ai_trading_system.backtest.daily import (
    DailyBacktestResult,
    build_backtest_data_credibility,
)
from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.config import BacktestRobustnessPolicyConfig
from ai_trading_system.scoring.position_model import (
    ModuleScore,
    PositionBandRule,
    WeightedScoreModel,
)


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
    weight_perturbation_pct: float
    random_seed_start: int
    random_seed_count: int
    oos_split_ratio: float
    policy_metadata: dict[str, object]
    policy: BacktestRobustnessPolicyConfig

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
    exposure: float,
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


def rebalance_interval_scenario(
    result: DailyBacktestResult,
    *,
    interval_days: int,
) -> BacktestRobustnessScenario:
    if interval_days <= 1:
        raise ValueError("rebalance interval_days must be greater than 1")
    metrics = _rebalance_interval_metrics(result, interval_days=interval_days)
    return BacktestRobustnessScenario(
        scenario_id=f"rebalance_every_{interval_days}d",
        label=f"每 {interval_days} 个交易日再平衡",
        category="rebalance_frequency",
        description=(
            f"只在首个 signal_date 和之后每 {interval_days} 个交易日读取一次"
            "基础动态目标仓位，其余日期维持上一仓位；复用同一下一交易日收益、"
            "显式成本假设和 point-in-time 输入。"
        ),
        metrics=metrics,
        first_signal_date=result.first_signal_date,
        last_signal_date=result.last_signal_date,
    )


def module_subset_baseline_scenario(
    result: DailyBacktestResult,
    *,
    scenario_id: str,
    label: str,
    modules: tuple[str, ...],
    weights: Mapping[str, float],
    position_bands: tuple[PositionBandRule, ...],
    category: str = "signal_family_baseline",
) -> BacktestRobustnessScenario:
    metrics = _module_subset_metrics(
        result,
        modules=modules,
        weights=weights,
        position_bands=position_bands,
    )
    module_label = " + ".join(modules)
    return BacktestRobustnessScenario(
        scenario_id=scenario_id,
        label=label,
        category=category,
        description=(
            f"只使用 {module_label} 模块分数和配置权重映射仓位，复用每日宏观总风险"
            "资产预算、最小调仓阈值、下一交易日收益和显式成本假设；不重跑 "
            "production scoring，也不新增数据源。"
        ),
        metrics=metrics,
        first_signal_date=result.first_signal_date,
        last_signal_date=result.last_signal_date,
    )


def same_turnover_random_scenario(
    result: DailyBacktestResult,
    *,
    seed: int,
) -> BacktestRobustnessScenario:
    metrics = _same_turnover_random_metrics(result, seed=seed)
    return BacktestRobustnessScenario(
        scenario_id=f"same_turnover_random_seed_{seed}",
        label=f"同换手率随机策略 seed {seed}",
        category="same_turnover_random_strategy",
        description=(
            f"使用固定随机种子 {seed} 生成随机加/减仓方向，但每日 absolute turnover "
            "与基础动态策略一致；逐日重新计算收益、融资和交易成本。"
        ),
        metrics=metrics,
        first_signal_date=result.first_signal_date,
        last_signal_date=result.last_signal_date,
    )


def same_exposure_random_scenario(
    result: DailyBacktestResult,
    *,
    seed: int,
) -> BacktestRobustnessScenario:
    rng = Random(seed)
    exposures = [row.raw_target_exposure for row in result.rows]
    rng.shuffle(exposures)
    metrics = _exposure_path_metrics(result, tuple(exposures))
    return BacktestRobustnessScenario(
        scenario_id=f"same_exposure_random_seed_{seed}",
        label=f"同 exposure 分布随机策略 seed {seed}",
        category="same_exposure_random_strategy",
        description=(
            f"使用固定随机种子 {seed} 打乱基础动态策略的每日目标 exposure，"
            "保留 exposure 分布但破坏信号时点；逐日重新计算收益和成本。"
        ),
        metrics=metrics,
        first_signal_date=result.first_signal_date,
        last_signal_date=result.last_signal_date,
    )


def model_target_no_gate_scenario(
    result: DailyBacktestResult,
) -> BacktestRobustnessScenario:
    exposures = tuple(row.total_asset_model_target_exposure for row in result.rows)
    metrics = _exposure_path_metrics(result, exposures)
    return BacktestRobustnessScenario(
        scenario_id="no_gate_model_target_baseline",
        label="No-gate model target 基线",
        category="score_architecture_baseline",
        description=(
            "使用线性加权模型映射出的 total_asset_model_target_exposure，"
            "不应用后续 position gate cap；复用同一下一交易日收益和成本假设。"
        ),
        metrics=metrics,
        first_signal_date=result.first_signal_date,
        last_signal_date=result.last_signal_date,
    )


def volatility_targeted_exposure_scenario(
    result: DailyBacktestResult,
    *,
    target_annual_volatility: float,
    lookback_days: int,
    fallback_exposure: float,
) -> BacktestRobustnessScenario:
    metrics = _volatility_targeted_metrics(
        result,
        target_annual_volatility=target_annual_volatility,
        lookback_days=lookback_days,
        fallback_exposure=fallback_exposure,
    )
    return BacktestRobustnessScenario(
        scenario_id="vol_targeted_total_asset_ai",
        label=f"Vol-targeted AI exposure {target_annual_volatility:.0%}",
        category="baseline",
        description=(
            f"使用前 {lookback_days} 个交易日 realized volatility 估计下一日 exposure，"
            f"目标年化波动 {target_annual_volatility:.0%}，不足样本时使用"
            f"固定 {fallback_exposure:.0%} 总资产 exposure；不读取未来收益。"
        ),
        metrics=metrics,
        first_signal_date=result.first_signal_date,
        last_signal_date=result.last_signal_date,
    )


def backtest_robustness_summary_record(
    report: BacktestRobustnessReport,
) -> dict[str, object]:
    result = report.base_result
    data_credibility = build_backtest_data_credibility(result)
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
        "data_credibility_grade": data_credibility.grade,
        "coverage_evidence": _coverage_evidence_record(result, report.policy),
        "weight_profile_version": result.weight_profile_version,
        "calibration_overlay_ids": list(result.calibration_overlay_ids),
        "effective_weights": result.effective_weights or {},
        "policy_metadata": report.policy_metadata,
        "policy": report.policy.model_dump(mode="json"),
        "cost_stress_increment_bps": report.cost_stress_increment_bps,
        "shifted_start_days": report.shifted_start_days,
        "weight_perturbation_pct": report.weight_perturbation_pct,
        "random_seed_start": report.random_seed_start,
        "random_seed_count": report.random_seed_count,
        "oos_split_ratio": report.oos_split_ratio,
        "base_dynamic": _summary_metrics_record(result.strategy_metrics),
        "scenarios": [
            _scenario_summary_record(scenario, result, report.policy)
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
        "remaining_gaps": (),
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
            f"- Policy version：{report.policy_metadata.get('version', 'unknown')}",
            f"- 成本压力增量：{report.cost_stress_increment_bps:.1f} bps",
            f"- 起点后移天数：{report.shifted_start_days}",
            f"- 模块权重扰动比例：±{report.weight_perturbation_pct:.0%}",
            (
                f"- 同换手率随机策略：seed {report.random_seed_start} 起，"
                f"{report.random_seed_count} 组"
            ),
            f"- 样本外切分比例：{report.oos_split_ratio:.0%} in-sample",
            "",
            "## 方法边界",
            "",
            (
                "成本压力、起点后移、权重扰动和样本外窗口复用同一批 "
                "point-in-time 输入，并重新调用同一评分/回测执行路径；固定总资产 "
                "AI exposure、再平衡频率、趋势信号族和随机策略属于执行或信号族"
                "基线。"
            ),
            (
                "成本压力实验只改变显式成本假设，不改变价格、基本面、估值、风险事件、"
                "观察池 lifecycle 或评分规则输入。"
            ),
            (
                "本报告不改变 production scoring、position_gate、日报结论或任何执行建议；"
                "完整生产信任仍需要历史估值/风险事件覆盖、随机基线和样本外验证。"
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

    lines.extend(_coverage_evidence_lines(report))

    lines.extend(
        [
            "",
            "## 初步解释",
            "",
            *_interpretation_lines(report),
            "",
            "## 剩余缺口",
            "",
            "- BACKTEST-001 已登记的稳健性实验缺口本阶段已覆盖。",
            (
                "- 历史估值快照和风险事件发生记录覆盖不足时，投资解释仍应降级；"
                "生产信任仍需 owner 审批。"
            ),
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
    if strategy.time_in_market < report.policy.full_exposure_time_in_market_min:
        lines.append(
            "- 动态策略不是长期满仓，仓位纪律和风险暴露管理应与收益指标分开评估。"
        )
    cost_scenario = _find_scenario(report, "cost_stress_execution")
    if cost_scenario is not None and cost_scenario.result is not None:
        delta = cost_scenario.result.strategy_metrics.total_return - strategy.total_return
        if delta < report.policy.material_cost_drag_total_return:
            lines.append(
                "- 成本压力对总收益造成明显拖累，生产前必须用真实成交样本校验成本假设。"
            )
        else:
            lines.append("- 成本压力实验未显著改变总收益方向，但仍不替代真实成交质量验证。")
    shifted_scenario = _find_scenario(report, "shifted_start")
    if shifted_scenario is not None and shifted_scenario.result is not None:
        delta = shifted_scenario.result.strategy_metrics.total_return - strategy.total_return
        if abs(delta) > report.policy.shifted_start_material_total_return_delta_abs:
            lines.append(
                "- 起点后移后结果变化较大，说明样本窗口敏感性仍是主要生产化风险。"
            )
        else:
            lines.append("- 起点后移后总收益变化有限，窗口敏感性在第一阶段实验中未明显恶化。")
    rebalance_scenarios = [
        scenario
        for scenario in report.scenarios
        if scenario.category == "rebalance_frequency" and scenario.metrics is not None
    ]
    if rebalance_scenarios:
        best_rebalance_delta = max(
            scenario.metrics.total_return - strategy.total_return
            for scenario in rebalance_scenarios
            if scenario.metrics is not None
        )
        if best_rebalance_delta >= 0:
            lines.append(
                "- 较低再平衡频率未降低本次总收益，动态仓位价值不能只归因于高频调仓。"
            )
        else:
            lines.append(
                "- 较低再平衡频率削弱本次总收益，需确认策略是否过度依赖每日调仓。"
            )
    signal_baselines = [
        scenario
        for scenario in report.scenarios
        if scenario.category == "signal_family_baseline" and scenario.metrics is not None
    ]
    if signal_baselines:
        best_signal_delta = max(
            scenario.metrics.total_return - strategy.total_return
            for scenario in signal_baselines
            if scenario.metrics is not None
        )
        if best_signal_delta > 0:
            lines.append(
                "- 至少一个趋势信号族基线跑赢动态策略，复杂模块增益仍需权重扰动和样本外验证。"
            )
        else:
            lines.append(
                "- 动态策略本次收益不低于趋势信号族基线，复杂模块未在该样本削弱总收益。"
            )
    weight_scenarios = [
        scenario
        for scenario in report.scenarios
        if scenario.category == "module_weight_perturbation"
        and scenario.result is not None
    ]
    if weight_scenarios:
        max_abs_delta = max(
            abs(scenario.result.strategy_metrics.total_return - strategy.total_return)
            for scenario in weight_scenarios
            if scenario.result is not None
        )
        if max_abs_delta > (
            report.policy.weight_perturbation_material_total_return_delta_abs
        ):
            lines.append(
                "- 模块权重扰动对总收益影响较大，当前参数仍需要样本外验证和 owner 审批。"
            )
        else:
            lines.append("- 模块权重扰动未明显改变本次总收益方向，参数敏感性初步可控。")
    random_scenarios = [
        scenario
        for scenario in report.scenarios
        if scenario.category == "same_turnover_random_strategy"
        and scenario.metrics is not None
    ]
    if random_scenarios:
        random_returns = [
            scenario.metrics.total_return
            for scenario in random_scenarios
            if scenario.metrics is not None
        ]
        random_beats = sum(1 for item in random_returns if item >= strategy.total_return)
        if random_beats == 0:
            lines.append(
                "- 动态策略跑赢全部同换手率随机策略，信号方向价值在本次随机基线中可见。"
            )
        else:
            lines.append(
                f"- {random_beats}/{len(random_returns)} 组同换手率随机策略不低于动态策略，"
                "信号显著性仍需更长样本和样本外验证。"
            )
    in_sample = _find_scenario(report, "in_sample_window")
    out_sample = _find_scenario(report, "out_of_sample_holdout")
    if in_sample is not None and out_sample is not None:
        in_metrics = (
            in_sample.result.strategy_metrics
            if in_sample.result is not None
            else in_sample.metrics
        )
        out_metrics = (
            out_sample.result.strategy_metrics
            if out_sample.result is not None
            else out_sample.metrics
        )
        if in_metrics is not None and out_metrics is not None:
            if out_metrics.total_return >= 0:
                lines.append(
                    "- 样本外 holdout 总收益为正，时间切分下未立即暴露方向性失效。"
                )
            else:
                lines.append(
                    "- 样本外 holdout 总收益为负，需把全样本结果降级为窗口内诊断。"
                )
            if out_metrics.total_return < (
                in_metrics.total_return
                - report.policy.oos_material_underperformance_total_return_delta
            ):
                lines.append(
                    "- 样本外收益明显弱于 in-sample，参数与市场阶段稳定性仍需人工复核。"
                )
    lines.append(
        "- 当前结论仍是研究和审计输入；完整生产信任还取决于数据可信度、样本长度和 owner 审批。"
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


def _exposure_path_metrics(
    result: DailyBacktestResult,
    exposures: tuple[float, ...],
) -> BacktestMetrics:
    if len(exposures) != len(result.rows):
        raise ValueError("exposure path length must match backtest rows")
    previous_exposure = 0.0
    returns: list[float] = []
    turnovers: list[float] = []
    for row, raw_exposure in zip(result.rows, exposures, strict=True):
        exposure = _clamp_exposure(raw_exposure)
        turnover = abs(exposure - previous_exposure)
        transaction_cost = _transaction_cost_for_turnover(
            result=result,
            exposure=exposure,
            previous_exposure=previous_exposure,
            turnover=turnover,
        )
        returns.append(exposure * row.asset_return - transaction_cost)
        turnovers.append(turnover)
        previous_exposure = exposure
    return summarize_long_only_backtest(
        strategy_returns=returns,
        exposures=[_clamp_exposure(value) for value in exposures],
        turnovers=turnovers,
    )


def _volatility_targeted_metrics(
    result: DailyBacktestResult,
    *,
    target_annual_volatility: float,
    lookback_days: int,
    fallback_exposure: float,
) -> BacktestMetrics:
    if not 0 < target_annual_volatility <= 1:
        raise ValueError("target annual volatility must satisfy 0 < value <= 1")
    if lookback_days <= 1:
        raise ValueError("volatility target lookback days must be greater than 1")
    if not 0 <= fallback_exposure <= 1:
        raise ValueError("fallback exposure must satisfy 0 <= exposure <= 1")
    target_daily_volatility = target_annual_volatility / sqrt(252.0)
    exposures: list[float] = []
    asset_returns: list[float] = []
    for row in result.rows:
        if len(asset_returns) < lookback_days:
            exposure = fallback_exposure
        else:
            trailing_returns = asset_returns[-lookback_days:]
            realized_volatility = pstdev(trailing_returns)
            exposure = (
                fallback_exposure
                if realized_volatility <= 0
                else target_daily_volatility / realized_volatility
            )
        exposures.append(_clamp_exposure(exposure))
        asset_returns.append(row.asset_return)
    return _exposure_path_metrics(result, tuple(exposures))


def _rebalance_interval_metrics(
    result: DailyBacktestResult,
    *,
    interval_days: int,
) -> BacktestMetrics:
    previous_exposure = 0.0
    current_exposure = 0.0
    returns: list[float] = []
    exposures: list[float] = []
    turnovers: list[float] = []
    for index, row in enumerate(result.rows):
        if index == 0 or index % interval_days == 0:
            desired_exposure = row.raw_target_exposure
            if index == 0 or abs(desired_exposure - previous_exposure) >= (
                result.minimum_action_delta
            ):
                current_exposure = desired_exposure
        turnover = abs(current_exposure - previous_exposure)
        transaction_cost = _transaction_cost_for_turnover(
            result=result,
            exposure=current_exposure,
            previous_exposure=previous_exposure,
            turnover=turnover,
        )
        returns.append(current_exposure * row.asset_return - transaction_cost)
        exposures.append(current_exposure)
        turnovers.append(turnover)
        previous_exposure = current_exposure
    return summarize_long_only_backtest(
        strategy_returns=returns,
        exposures=exposures,
        turnovers=turnovers,
    )


def _module_subset_metrics(
    result: DailyBacktestResult,
    *,
    modules: tuple[str, ...],
    weights: Mapping[str, float],
    position_bands: tuple[PositionBandRule, ...],
) -> BacktestMetrics:
    if not modules:
        raise ValueError("module subset must not be empty")
    if len(set(modules)) != len(modules):
        raise ValueError("module subset must not contain duplicates")

    score_model = WeightedScoreModel(position_bands=position_bands)
    previous_exposure = 0.0
    returns: list[float] = []
    exposures: list[float] = []
    turnovers: list[float] = []
    for row in result.rows:
        components: list[ModuleScore] = []
        for module in modules:
            if module not in row.component_scores:
                raise ValueError(f"missing component score for module {module!r}")
            if module not in weights:
                raise ValueError(f"missing scoring weight for module {module!r}")
            components.append(
                ModuleScore(
                    name=module,
                    score=row.component_scores[module],
                    weight=weights[module],
                    reason="backtest_robustness_signal_family_baseline",
                )
            )
        recommendation = score_model.recommend(
            components,
            total_risk_asset_min=row.total_risk_asset_min,
            total_risk_asset_max=row.total_risk_asset_max,
        )
        desired_exposure = _position_midpoint(
            recommendation.total_asset_ai_band.min_position,
            recommendation.total_asset_ai_band.max_position,
        )
        if not returns or abs(desired_exposure - previous_exposure) >= (
            result.minimum_action_delta
        ):
            current_exposure = desired_exposure
        else:
            current_exposure = previous_exposure
        turnover = abs(current_exposure - previous_exposure)
        transaction_cost = _transaction_cost_for_turnover(
            result=result,
            exposure=current_exposure,
            previous_exposure=previous_exposure,
            turnover=turnover,
        )
        returns.append(current_exposure * row.asset_return - transaction_cost)
        exposures.append(current_exposure)
        turnovers.append(turnover)
        previous_exposure = current_exposure
    return summarize_long_only_backtest(
        strategy_returns=returns,
        exposures=exposures,
        turnovers=turnovers,
    )


def _same_turnover_random_metrics(
    result: DailyBacktestResult,
    *,
    seed: int,
) -> BacktestMetrics:
    exposures = _same_turnover_random_exposures(
        tuple(row.turnover for row in result.rows),
        seed=seed,
    )
    previous_exposure = 0.0
    returns: list[float] = []
    turnovers: list[float] = []
    for row, exposure in zip(result.rows, exposures, strict=True):
        turnover = abs(exposure - previous_exposure)
        transaction_cost = _transaction_cost_for_turnover(
            result=result,
            exposure=exposure,
            previous_exposure=previous_exposure,
            turnover=turnover,
        )
        returns.append(exposure * row.asset_return - transaction_cost)
        turnovers.append(turnover)
        previous_exposure = exposure
    return summarize_long_only_backtest(
        strategy_returns=returns,
        exposures=list(exposures),
        turnovers=turnovers,
    )


def _same_turnover_random_exposures(
    turnovers: tuple[float, ...],
    *,
    seed: int,
) -> tuple[float, ...]:
    rng = Random(seed)
    states = {0.0}
    predecessor_steps: list[dict[float, list[float]]] = []
    for turnover in turnovers:
        step_predecessors: dict[float, list[float]] = {}
        rounded_turnover = _round_exposure(turnover)
        for previous in states:
            for direction in (-1.0, 1.0):
                candidate = previous + direction * rounded_turnover
                if -1e-9 <= candidate <= 1.0 + 1e-9:
                    exposure = _round_exposure(_clamp_exposure(candidate))
                    step_predecessors.setdefault(exposure, []).append(previous)
        if not step_predecessors:
            raise ValueError(
                "same-turnover random path is infeasible for the base turnover sequence"
            )
        predecessor_steps.append(step_predecessors)
        states = set(step_predecessors)

    state = rng.choice(sorted(states))
    reversed_path: list[float] = []
    for predecessors in reversed(predecessor_steps):
        reversed_path.append(state)
        state = rng.choice(sorted(predecessors[state]))
    return tuple(reversed(reversed_path))


def _transaction_cost_for_turnover(
    *,
    result: DailyBacktestResult,
    exposure: float,
    previous_exposure: float,
    turnover: float,
    cost_bps: float | None = None,
    spread_bps: float | None = None,
    slippage_bps: float | None = None,
    market_impact_bps: float | None = None,
    tax_bps: float | None = None,
    fx_bps: float | None = None,
    financing_annual_bps: float | None = None,
    etf_delay_bps: float | None = None,
) -> float:
    commission_rate = (result.cost_bps if cost_bps is None else cost_bps) / 10_000.0
    spread_rate = (result.spread_bps if spread_bps is None else spread_bps) / 10_000.0
    slippage_rate = (
        result.slippage_bps if slippage_bps is None else slippage_bps
    ) / 10_000.0
    market_impact_rate = (
        result.market_impact_bps if market_impact_bps is None else market_impact_bps
    ) / 10_000.0
    tax_rate = (result.tax_bps if tax_bps is None else tax_bps) / 10_000.0
    fx_rate = (result.fx_bps if fx_bps is None else fx_bps) / 10_000.0
    financing_daily_rate = (
        result.financing_annual_bps
        if financing_annual_bps is None
        else financing_annual_bps
    ) / 10_000.0 / 252.0
    etf_delay_rate = (
        result.etf_delay_bps if etf_delay_bps is None else etf_delay_bps
    ) / 10_000.0
    sell_turnover = max(previous_exposure - exposure, 0.0)
    return (
        turnover * commission_rate
        + turnover * spread_rate
        + turnover * slippage_rate
        + turnover * market_impact_rate
        + sell_turnover * tax_rate
        + turnover * fx_rate
        + exposure * financing_daily_rate
        + turnover * etf_delay_rate
    )


def _scenario_summary_record(
    scenario: BacktestRobustnessScenario,
    base_result: DailyBacktestResult,
    policy: BacktestRobustnessPolicyConfig,
) -> dict[str, object]:
    base_metrics = base_result.strategy_metrics
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
        bootstrap_ci = _scenario_bootstrap_ci_record(
            scenario=scenario,
            base_result=base_result,
            policy=policy,
        )
        if bootstrap_ci is not None:
            record["return_delta_bootstrap_ci_95"] = bootstrap_ci
    if scenario.result is not None:
        record["weight_profile_version"] = scenario.result.weight_profile_version
        record["calibration_overlay_ids"] = list(
            scenario.result.calibration_overlay_ids
        )
        record["effective_weights"] = scenario.result.effective_weights or {}
    return record


def _scenario_bootstrap_ci_record(
    *,
    scenario: BacktestRobustnessScenario,
    base_result: DailyBacktestResult,
    policy: BacktestRobustnessPolicyConfig,
) -> dict[str, object] | None:
    if scenario.result is None:
        return None
    base_returns = tuple(row.strategy_return for row in base_result.rows)
    scenario_returns = tuple(row.strategy_return for row in scenario.result.rows)
    if len(base_returns) != len(scenario_returns) or len(base_returns) < 2:
        return None
    ci_low, ci_high = _block_bootstrap_total_return_delta_ci(
        base_returns=base_returns,
        scenario_returns=scenario_returns,
        iterations=policy.bootstrap_iterations,
        block_size_days=policy.bootstrap_block_size_days,
        random_seed=policy.bootstrap_random_seed
        + sum(ord(character) for character in scenario.scenario_id),
    )
    return {
        "method": "paired_block_bootstrap_total_return_delta",
        "iterations": policy.bootstrap_iterations,
        "block_size_days": policy.bootstrap_block_size_days,
        "daily_return_count": len(base_returns),
        "low": ci_low,
        "high": ci_high,
    }


def _block_bootstrap_total_return_delta_ci(
    *,
    base_returns: tuple[float, ...],
    scenario_returns: tuple[float, ...],
    iterations: int,
    block_size_days: int,
    random_seed: int,
) -> tuple[float, float]:
    if iterations <= 0:
        raise ValueError("bootstrap iterations must be positive")
    if block_size_days <= 0:
        raise ValueError("bootstrap block size must be positive")
    rng = Random(random_seed)
    count = len(base_returns)
    deltas: list[float] = []
    for _ in range(iterations):
        sampled_base: list[float] = []
        sampled_scenario: list[float] = []
        while len(sampled_base) < count:
            start = rng.randrange(count)
            for offset in range(block_size_days):
                index = (start + offset) % count
                sampled_base.append(base_returns[index])
                sampled_scenario.append(scenario_returns[index])
                if len(sampled_base) >= count:
                    break
        deltas.append(
            _compound_return(sampled_scenario) - _compound_return(sampled_base)
        )
    ordered = sorted(deltas)
    return _quantile(ordered, 0.025), _quantile(ordered, 0.975)


def _compound_return(returns: list[float]) -> float:
    value = 1.0
    for item in returns:
        value *= 1.0 + item
    return value - 1.0


def _quantile(sorted_values: list[float], probability: float) -> float:
    if not sorted_values:
        raise ValueError("sorted_values must not be empty")
    if not 0 <= probability <= 1:
        raise ValueError("probability must be between 0 and 1")
    position = (len(sorted_values) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def _coverage_evidence_record(
    result: DailyBacktestResult,
    policy: BacktestRobustnessPolicyConfig,
) -> dict[str, object]:
    row_count = len(result.rows)
    components = sorted(
        {
            component
            for row in result.rows
            for component in (
                set(row.component_coverages) | set(row.component_source_types)
            )
        }
    )
    blocking_source_types = set(policy.candidate_blocking_component_source_types)
    component_records: dict[str, object] = {}
    blocking_components: list[str] = []
    minimum_component_coverage: float | None = None
    minimum_average_component_coverage: float | None = None
    maximum_placeholder_share: float | None = None

    for component in components:
        coverage_values = [
            float(row.component_coverages.get(component, 0.0))
            for row in result.rows
        ]
        source_type_counts: dict[str, int] = {}
        for row in result.rows:
            source_type = row.component_source_types.get(component, "missing")
            source_type_counts[source_type] = source_type_counts.get(source_type, 0) + 1
        observations = len(coverage_values)
        component_min = min(coverage_values) if coverage_values else None
        component_avg = (
            sum(coverage_values) / len(coverage_values) if coverage_values else None
        )
        placeholder_count = source_type_counts.get("placeholder", 0)
        insufficient_count = source_type_counts.get("insufficient_data", 0)
        placeholder_share = (
            None if observations == 0 else placeholder_count / observations
        )
        blocking_source_count = sum(
            count
            for source_type, count in source_type_counts.items()
            if source_type in blocking_source_types
        )
        component_blocked = (
            component_min is None
            or component_avg is None
            or component_min < policy.candidate_min_component_coverage
            or component_avg < policy.candidate_min_component_coverage
            or (
                placeholder_share is not None
                and placeholder_share > policy.candidate_max_placeholder_share
            )
            or blocking_source_count > 0
        )
        if component_blocked:
            blocking_components.append(component)
        minimum_component_coverage = (
            component_min
            if minimum_component_coverage is None
            else min(minimum_component_coverage, component_min or 0.0)
        )
        minimum_average_component_coverage = (
            component_avg
            if minimum_average_component_coverage is None
            else min(minimum_average_component_coverage, component_avg or 0.0)
        )
        maximum_placeholder_share = (
            placeholder_share
            if maximum_placeholder_share is None
            else max(maximum_placeholder_share, placeholder_share or 0.0)
        )
        component_records[component] = {
            "observations": observations,
            "min_coverage": component_min,
            "average_coverage": component_avg,
            "placeholder_count": placeholder_count,
            "insufficient_data_count": insufficient_count,
            "placeholder_share": placeholder_share,
            "blocking_source_count": blocking_source_count,
            "source_type_counts": source_type_counts,
            "blocked": component_blocked,
        }

    return {
        "available": bool(components),
        "sample_count": row_count,
        "min_required_component_coverage": policy.candidate_min_component_coverage,
        "max_allowed_placeholder_share": policy.candidate_max_placeholder_share,
        "blocking_source_types": sorted(blocking_source_types),
        "minimum_component_coverage": minimum_component_coverage,
        "minimum_average_component_coverage": minimum_average_component_coverage,
        "maximum_placeholder_share": maximum_placeholder_share,
        "blocking_components": blocking_components,
        "blocked": bool(blocking_components),
        "components": component_records,
    }


def _coverage_evidence_lines(report: BacktestRobustnessReport) -> list[str]:
    evidence = _coverage_evidence_record(report.base_result, report.policy)
    components = evidence.get("components")
    if not isinstance(components, dict) or not components:
        return [
            "",
            "## 数据覆盖与 source veto 证据",
            "",
            "- 未找到 component coverage 证据；参数候选不得据此晋级。",
        ]
    lines = [
        "",
        "## 数据覆盖与 source veto 证据",
        "",
        (
            "- 候选最低模块覆盖率："
            f"{float(evidence['min_required_component_coverage']):.0%}"
        ),
        (
            "- 候选最大 placeholder 占比："
            f"{float(evidence['max_allowed_placeholder_share']):.0%}"
        ),
        (
            "- 阻断 source_type："
            + ", ".join(str(item) for item in evidence["blocking_source_types"])
        ),
        "| 模块 | min coverage | avg coverage | placeholder share | blocking sources | 状态 |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for component, raw_record in sorted(components.items()):
        record = raw_record if isinstance(raw_record, dict) else {}
        status = "BLOCK" if record.get("blocked") else "PASS"
        lines.append(
            f"| `{component}` | "
            f"{_format_pct(_float_or_none(record.get('min_coverage')))} | "
            f"{_format_pct(_float_or_none(record.get('average_coverage')))} | "
            f"{_format_pct(_float_or_none(record.get('placeholder_share')))} | "
            f"{int(record.get('blocking_source_count') or 0)} | "
            f"{status} |"
        )
    return lines


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


def _format_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1%}"


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_optional(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _position_midpoint(min_position: float, max_position: float) -> float:
    return (min_position + max_position) / 2.0


def _clamp_exposure(value: float) -> float:
    return min(max(value, 0.0), 1.0)


def _round_exposure(value: float) -> float:
    return round(value, 10)
