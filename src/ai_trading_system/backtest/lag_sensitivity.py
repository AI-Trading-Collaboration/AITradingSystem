from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ai_trading_system.backtest.daily import (
    DailyBacktestResult,
    build_backtest_data_credibility,
)
from ai_trading_system.backtest.engine import BacktestMetrics


@dataclass(frozen=True)
class BacktestLagSensitivityScenario:
    feature_lag_days: int
    universe_lag_days: int
    rebalance_delay_days: int
    result: DailyBacktestResult | None = None
    skipped_reason: str | None = None

    @property
    def scenario_id(self) -> str:
        return f"feature_lag_{self.feature_lag_days}_universe_lag_{self.universe_lag_days}"

    @property
    def status(self) -> str:
        if self.skipped_reason:
            return "SKIPPED"
        if self.result is None:
            return "NOT_RUN"
        return self.result.status


@dataclass(frozen=True)
class BacktestLagSensitivityReport:
    base_result: DailyBacktestResult
    scenarios: tuple[BacktestLagSensitivityScenario, ...]
    tested_lag_days: tuple[int, ...]

    @property
    def status(self) -> str:
        completed = [scenario for scenario in self.scenarios if scenario.result is not None]
        if not completed:
            return "INSUFFICIENT_SCENARIOS"
        return "PASS_WITH_LIMITATIONS"


def default_backtest_lag_sensitivity_report_path(
    output_dir: Path,
    start: date,
    end: date,
) -> Path:
    return output_dir / f"backtest_lag_sensitivity_{start.isoformat()}_{end.isoformat()}.md"


def default_backtest_lag_sensitivity_summary_path(
    output_dir: Path,
    start: date,
    end: date,
) -> Path:
    return output_dir / f"backtest_lag_sensitivity_{start.isoformat()}_{end.isoformat()}.json"


def write_backtest_lag_sensitivity_report(
    report: BacktestLagSensitivityReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_backtest_lag_sensitivity_report(report), encoding="utf-8")
    return output_path


def write_backtest_lag_sensitivity_summary(
    report: BacktestLagSensitivityReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            backtest_lag_sensitivity_summary_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return output_path


def backtest_lag_sensitivity_summary_record(
    report: BacktestLagSensitivityReport,
) -> dict[str, object]:
    result = report.base_result
    credibility = build_backtest_data_credibility(result)
    return {
        "schema_version": 1,
        "report_type": "backtest_lag_sensitivity",
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
        "backtest_data_quality": {
            "grade": credibility.grade,
            "label": credibility.label,
            "uses_vendor_historical_estimates": (
                credibility.uses_vendor_historical_estimates
            ),
            "uses_self_archived_snapshots": credibility.uses_self_archived_snapshots,
            "universe_pit": credibility.universe_pit,
        },
        "tested_lag_days": list(report.tested_lag_days),
        "rebalance_delay_days": result.rebalance_delay_days,
        "base_dynamic": _summary_metrics_record(result.strategy_metrics),
        "scenarios": [
            _scenario_summary_record(scenario, result.strategy_metrics)
            for scenario in report.scenarios
        ],
        "interpretation": _interpretation_summary(report),
    }


def render_backtest_lag_sensitivity_report(
    report: BacktestLagSensitivityReport,
) -> str:
    result = report.base_result
    credibility = build_backtest_data_credibility(result)
    lines = [
        "# 回测滞后敏感性报告",
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
            f"- Backtest Data Quality：{credibility.label}（{credibility.grade}）",
            f"- 测试滞后天数：{', '.join(str(value) for value in report.tested_lag_days)}",
            f"- Rebalance Delay：{result.rebalance_delay_days} 个交易日",
            "",
            "## 方法边界",
            "",
            (
                "本报告复用同一数据质量门禁、同一交易成本、同一回测收益生效规则和"
                "同一观察池 lifecycle；每个场景只改变评分特征与 universe 可见性的交易日滞后。"
            ),
            (
                "feature lag 表示信号日使用更早交易日的特征、SEC/估值/风险事件 PIT 切片；"
                "universe lag 表示观察池按更早交易日的 lifecycle 可见性过滤。"
            ),
            (
                "C 级回测下，Sharpe 和收益指标只作为探索性诊断；不能作为无条件绩效结论。"
            ),
            "",
            "## 场景结果",
            "",
            (
                "| 场景 | Feature Lag | Universe Lag | Rebalance Delay | 状态 | 信号区间 | "
                "总收益 | 最大回撤 | Sharpe | 相对基础收益 | 解释 |"
            ),
            "|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|",
            _result_row(
                label="base_lag_0",
                feature_lag_days=result.feature_lag_days,
                universe_lag_days=result.universe_lag_days,
                rebalance_delay_days=result.rebalance_delay_days,
                status=result.status,
                metrics=result.strategy_metrics,
                first_signal_date=result.first_signal_date,
                last_signal_date=result.last_signal_date,
                base_metrics=result.strategy_metrics,
                note="基础动态策略，未额外延迟特征或 universe 输入。",
            ),
        ]
    )
    for scenario in report.scenarios:
        lines.append(_scenario_row(scenario, result.strategy_metrics))

    lines.extend(["", "## 初步解释", ""])
    lines.extend(f"- {item}" for item in _interpretation_summary(report))
    return "\n".join(lines) + "\n"


def _scenario_row(
    scenario: BacktestLagSensitivityScenario,
    base_metrics: BacktestMetrics,
) -> str:
    if scenario.result is None:
        note = scenario.skipped_reason or "未运行。"
        return (
            f"| {scenario.scenario_id} | {scenario.feature_lag_days} | "
            f"{scenario.universe_lag_days} | {scenario.rebalance_delay_days} | "
            f"{scenario.status} | n/a | n/a | n/a | n/a | n/a | {note} |"
        )
    return _result_row(
        label=scenario.scenario_id,
        feature_lag_days=scenario.feature_lag_days,
        universe_lag_days=scenario.universe_lag_days,
        rebalance_delay_days=scenario.rebalance_delay_days,
        status=scenario.status,
        metrics=scenario.result.strategy_metrics,
        first_signal_date=scenario.result.first_signal_date,
        last_signal_date=scenario.result.last_signal_date,
        base_metrics=base_metrics,
        note=_lag_interpretation(scenario.result.strategy_metrics),
    )


def _result_row(
    *,
    label: str,
    feature_lag_days: int,
    universe_lag_days: int,
    rebalance_delay_days: int,
    status: str,
    metrics: BacktestMetrics,
    first_signal_date: date,
    last_signal_date: date,
    base_metrics: BacktestMetrics,
    note: str,
) -> str:
    return (
        f"| {label} | {feature_lag_days} | {universe_lag_days} | "
        f"{rebalance_delay_days} | {status} | {first_signal_date.isoformat()} 至 "
        f"{last_signal_date.isoformat()} | {_format_percent(metrics.total_return)} | "
        f"{_format_percent(metrics.max_drawdown)} | {_format_optional(metrics.sharpe)} | "
        f"{_format_percent(metrics.total_return - base_metrics.total_return)} | {note} |"
    )


def _interpretation_summary(report: BacktestLagSensitivityReport) -> list[str]:
    result = report.base_result
    credibility = build_backtest_data_credibility(result)
    base_effective = _is_effective(result.strategy_metrics)
    effective_lagged = [
        scenario
        for scenario in report.scenarios
        if scenario.result is not None
        and max(scenario.feature_lag_days, scenario.universe_lag_days) >= 3
        and _is_effective(scenario.result.strategy_metrics)
    ]
    lines: list[str] = []
    if credibility.grade == "C":
        lines.append(
            "基础回测数据可信度为 C 级；本报告只能帮助识别未来函数风险，不能解除输入缺口。"
        )
    if base_effective and not effective_lagged:
        lines.append(
            "策略只在低滞后或 lag=0 场景有效，未来函数或过度依赖即时输入的风险较高。"
        )
    elif effective_lagged:
        lines.append(
            "至少一个 3 个交易日以上滞后场景仍保持正收益和正 Sharpe，可作为更可信研究线索。"
        )
    else:
        lines.append("基础场景本身未达到正收益和正 Sharpe，有效性需要重新评估。")
    lines.append(
        "完整生产信任仍需要更完整的 PIT 估值/风险事件覆盖、权重扰动和样本外验证。"
    )
    return lines


def _lag_interpretation(metrics: BacktestMetrics) -> str:
    if _is_effective(metrics):
        return "正收益且 Sharpe 为正。"
    return "未同时满足正收益和正 Sharpe。"


def _is_effective(metrics: BacktestMetrics) -> bool:
    return metrics.total_return > 0 and metrics.sharpe is not None and metrics.sharpe > 0


def _scenario_summary_record(
    scenario: BacktestLagSensitivityScenario,
    base_metrics: BacktestMetrics,
) -> dict[str, object]:
    record: dict[str, object] = {
        "scenario_id": scenario.scenario_id,
        "feature_lag_days": scenario.feature_lag_days,
        "universe_lag_days": scenario.universe_lag_days,
        "rebalance_delay_days": scenario.rebalance_delay_days,
        "status": scenario.status,
        "skipped_reason": scenario.skipped_reason,
    }
    if scenario.result is not None:
        record.update(_summary_metrics_record(scenario.result.strategy_metrics))
        record["first_signal_date"] = scenario.result.first_signal_date.isoformat()
        record["last_signal_date"] = scenario.result.last_signal_date.isoformat()
        record["total_return_delta_vs_base"] = (
            scenario.result.strategy_metrics.total_return - base_metrics.total_return
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


def _format_percent(value: float) -> str:
    return f"{value:.1%}"


def _format_optional(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"
