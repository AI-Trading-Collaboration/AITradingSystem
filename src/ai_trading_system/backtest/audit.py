from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

from ai_trading_system.backtest.daily import (
    DailyBacktestResult,
    build_backtest_data_credibility,
)
from ai_trading_system.scoring.daily import COMPONENT_LABELS, SOURCE_TYPE_LABELS

AuditSeverity = Literal["ERROR", "WARNING"]

_SOURCE_TYPES_REQUIRING_REVIEW = frozenset(
    {"partial_hard_data", "partial_manual_input", "insufficient_data", "placeholder"}
)


@dataclass(frozen=True)
class BacktestAuditIssue:
    severity: AuditSeverity
    code: str
    subject: str
    message: str
    recommendation: str


@dataclass(frozen=True)
class BacktestAuditReport:
    result: DailyBacktestResult
    issues: tuple[BacktestAuditIssue, ...]
    data_quality_report_path: Path
    backtest_report_path: Path
    daily_output_path: Path
    input_coverage_output_path: Path
    sec_companyfacts_validation_report_path: Path | None
    minimum_component_coverage: float

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "WARNING")

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


def build_backtest_audit_report(
    result: DailyBacktestResult,
    data_quality_report_path: Path,
    backtest_report_path: Path,
    daily_output_path: Path,
    input_coverage_output_path: Path,
    sec_companyfacts_validation_report_path: Path | None = None,
    minimum_component_coverage: float = 0.9,
) -> BacktestAuditReport:
    if not 0.0 <= minimum_component_coverage <= 1.0:
        raise ValueError("minimum_component_coverage 必须在 0 到 1 之间")

    issues: list[BacktestAuditIssue] = []
    _append_data_quality_issues(issues, result)
    _append_point_in_time_input_issues(issues, result)
    _append_data_credibility_issues(issues, result)
    _append_component_coverage_issues(issues, result, minimum_component_coverage)
    _append_source_type_issues(issues, result)
    _append_input_issue_summaries(issues, result)

    return BacktestAuditReport(
        result=result,
        issues=tuple(issues),
        data_quality_report_path=data_quality_report_path,
        backtest_report_path=backtest_report_path,
        daily_output_path=daily_output_path,
        input_coverage_output_path=input_coverage_output_path,
        sec_companyfacts_validation_report_path=sec_companyfacts_validation_report_path,
        minimum_component_coverage=minimum_component_coverage,
    )


def render_backtest_audit_report(report: BacktestAuditReport) -> str:
    result = report.result
    lines = [
        "# 回测输入审计报告",
        "",
        f"- 审计状态：{report.status}",
        f"- 错误数：{report.error_count}；警告数：{report.warning_count}",
        f"- 请求区间：{result.requested_start.isoformat()} 至 {result.requested_end.isoformat()}",
        (
            f"- 实际信号区间：{result.first_signal_date.isoformat()} "
            f"至 {result.last_signal_date.isoformat()}"
        ),
        f"- 策略代理标的：{result.strategy_ticker}",
        f"- 基准：{', '.join(result.benchmark_tickers)}",
        f"- 回测状态：{result.status}",
        f"- 回测报告：`{report.backtest_report_path}`",
        f"- 每日明细：`{report.daily_output_path}`",
        f"- 输入覆盖诊断：`{report.input_coverage_output_path}`",
        f"- 数据质量报告：`{report.data_quality_report_path}`",
    ]
    if report.sec_companyfacts_validation_report_path is not None:
        lines.append(
            f"- SEC companyfacts 校验报告：`{report.sec_companyfacts_validation_report_path}`"
        )
    if result.market_regime is not None:
        lines.extend(
            [
                f"- 市场阶段：{result.market_regime.name}（{result.market_regime.regime_id}）",
                (
                    f"- 锚定事件：{result.market_regime.anchor_date.isoformat()} "
                    f"{result.market_regime.anchor_event}"
                ),
            ]
        )

    lines.extend(_data_quality_section(report))
    lines.extend(_data_credibility_section(report))
    lines.extend(_point_in_time_section(report))
    lines.extend(_component_coverage_section(report))
    lines.extend(_source_type_section(report))
    lines.extend(_execution_section(report))
    lines.extend(_issue_section(report))
    lines.extend(_recommended_action_section(report))
    return "\n".join(lines) + "\n"


def write_backtest_audit_report(report: BacktestAuditReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_backtest_audit_report(report), encoding="utf-8")
    return output_path


def default_backtest_audit_report_path(output_dir: Path, start: date, end: date) -> Path:
    return output_dir / f"backtest_audit_{start.isoformat()}_{end.isoformat()}.md"


def _append_data_quality_issues(
    issues: list[BacktestAuditIssue],
    result: DailyBacktestResult,
) -> None:
    quality = result.data_quality_report
    if not quality.passed:
        issues.append(
            BacktestAuditIssue(
                severity="ERROR",
                code="data_quality_gate_failed",
                subject="market_data",
                message="市场数据质量门禁失败，本次回测不能用于结论解释。",
                recommendation=(
                    "先修复 prices_daily.csv、rates_daily.csv 或下载 manifest "
                    "问题后重跑。"
                ),
            )
        )
        return
    if quality.warning_count:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="data_quality_gate_warnings",
                subject="market_data",
                message=f"市场数据质量门禁通过但有 {quality.warning_count} 个警告。",
                recommendation="解释回测结论前先查看数据质量报告中的新鲜度、缺口和异常值警告。",
            )
        )


def _append_point_in_time_input_issues(
    issues: list[BacktestAuditIssue],
    result: DailyBacktestResult,
) -> None:
    signal_count = len(result.rows)
    if result.fundamental_feature_report_count != signal_count:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="sec_point_in_time_slice_incomplete",
                subject="SEC 基本面",
                message=(
                    f"SEC 基本面切片数 {result.fundamental_feature_report_count} "
                    f"低于信号日数 {signal_count}。"
                ),
                recommendation="补齐 SEC companyfacts 缓存或检查 point-in-time 指标构建路径。",
            )
        )
    if result.valuation_review_report_count != signal_count:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="valuation_point_in_time_slice_incomplete",
                subject="估值快照",
                message=(
                    f"估值快照切片数 {result.valuation_review_report_count} "
                    f"低于信号日数 {signal_count}。"
                ),
                recommendation="检查估值 YAML/CSV 导入历史是否覆盖回测区间。",
            )
        )
    if result.risk_event_occurrence_review_report_count != signal_count:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="risk_event_point_in_time_slice_incomplete",
                subject="风险事件发生记录",
                message=(
                    f"风险事件发生记录切片数 "
                    f"{result.risk_event_occurrence_review_report_count} 低于信号日数 "
                    f"{signal_count}。"
                ),
                recommendation="检查风险事件发生记录目录和历史证据 captured_at/published_at。",
            )
        )
    if result.fundamental_feature_row_count_min == 0:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="sec_feature_rows_empty",
                subject="SEC 基本面",
                message="至少一个 signal_date 没有可用 SEC 基本面特征行。",
                recommendation="查看输入覆盖诊断中的 ticker_sec_feature 和 SEC 缺失观测。",
            )
        )
    if result.valuation_snapshot_count_max == 0:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="valuation_history_empty",
                subject="估值快照",
                message="整个回测区间没有可见估值快照。",
                recommendation="导入历史估值/预期快照，或明确该回测只评估无估值模块的策略。",
            )
        )
    if result.risk_event_occurrence_count_max == 0:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="risk_event_history_empty",
                subject="风险事件发生记录",
                message="整个回测区间没有可见风险事件发生记录。",
                recommendation="确认这代表历史无已触发事件，而不是风险事件库尚未建设。",
            )
        )


def _append_data_credibility_issues(
    issues: list[BacktestAuditIssue],
    result: DailyBacktestResult,
) -> None:
    credibility = build_backtest_data_credibility(result)
    if credibility.grade == "C":
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="backtest_data_quality_c_exploratory",
                subject="Backtest Data Quality",
                message=(
                    "本次回测被标记为 C 级探索性输入，不能输出无条件 Sharpe 或绩效结论。"
                ),
                recommendation=(
                    "先补齐 PIT 输入覆盖、运行 lag sensitivity，并在报告中逐项说明降级原因。"
                ),
            )
        )
    elif credibility.grade == "B":
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="backtest_data_quality_b_requires_lag_review",
                subject="Backtest Data Quality",
                message="本次回测为 B 级保守近似，需要结合 lag sensitivity 解释。",
                recommendation="解释策略有效性前检查 3-5 个交易日滞后后是否仍保持方向。",
            )
        )


def _append_component_coverage_issues(
    issues: list[BacktestAuditIssue],
    result: DailyBacktestResult,
    minimum_component_coverage: float,
) -> None:
    for component, values in sorted(_component_coverage_values(result).items()):
        average = sum(values) / len(values)
        minimum = min(values)
        if average < minimum_component_coverage or minimum <= 0:
            issues.append(
                BacktestAuditIssue(
                    severity="WARNING",
                    code="component_coverage_below_threshold",
                    subject=component,
                    message=(
                        f"{_component_label(component)} 平均覆盖率 {average:.0%}，"
                        f"最低覆盖率 {minimum:.0%}，低于审计阈值 "
                        f"{minimum_component_coverage:.0%}。"
                    ),
                    recommendation=(
                        "查看 backtest_input_coverage CSV 中的 "
                        "component_coverage 记录。"
                    ),
                )
            )


def _append_source_type_issues(
    issues: list[BacktestAuditIssue],
    result: DailyBacktestResult,
) -> None:
    requiring_review = _component_source_type_counts(result)
    for (component, source_type), count in sorted(requiring_review.items()):
        if source_type not in _SOURCE_TYPES_REQUIRING_REVIEW:
            continue
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="component_source_type_requires_review",
                subject=f"{component}:{source_type}",
                message=(
                    f"{_component_label(component)} 有 {count} 个 signal_date 使用 "
                    f"{_source_type_label(source_type)}。"
                ),
                recommendation="解释绩效前先确认这些日期是否因数据不足或占位输入影响分数。",
            )
        )

    public_valuation = _source_type_count(
        result.monthly_valuation_source_type_counts,
        "public_convenience",
    )
    if public_valuation:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="valuation_public_convenience_present",
                subject="估值快照",
                message=f"估值快照中出现 {public_valuation} 条公开便利源记录。",
                recommendation="公开便利源只能辅助说明，不能作为自动评分主依据。",
            )
        )

    public_risk = _source_type_count(
        result.monthly_risk_event_source_type_counts,
        "public_convenience",
    )
    if public_risk:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="risk_event_public_convenience_present",
                subject="风险事件发生记录",
                message=f"风险事件证据中出现 {public_risk} 条公开便利源记录。",
                recommendation="补充一手来源、付费供应商或手工审计输入后再解释风险评分。",
            )
        )


def _append_input_issue_summaries(
    issues: list[BacktestAuditIssue],
    result: DailyBacktestResult,
) -> None:
    if not result.monthly_input_issue_counts:
        return

    top_issues = sorted(
        result.monthly_input_issue_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )[:10]
    for (month, input_label, code, subject), count in top_issues:
        issues.append(
            BacktestAuditIssue(
                severity="WARNING",
                code="historical_input_issue_present",
                subject=f"{month}:{input_label}:{subject}",
                message=f"{month} 的 {input_label} 出现 {count} 次 {code}。",
                recommendation="查看回测报告的月度输入问题下钻和输入覆盖诊断 CSV。",
            )
        )


def _data_quality_section(report: BacktestAuditReport) -> list[str]:
    quality = report.result.data_quality_report
    return [
        "",
        "## 数据质量门禁",
        "",
        f"- 状态：{quality.status}",
        f"- 错误数：{quality.error_count}；警告数：{quality.warning_count}",
        f"- 价格缓存行数：{quality.price_summary.rows}",
        f"- 利率缓存行数：{quality.rate_summary.rows}",
        (
            f"- 下载审计清单行数："
            f"{quality.manifest_summary.rows if quality.manifest_summary else 'n/a'}"
        ),
    ]


def _data_credibility_section(report: BacktestAuditReport) -> list[str]:
    credibility = build_backtest_data_credibility(report.result)
    lines = [
        "",
        "## 回测数据可信度",
        "",
        f"- Backtest Data Quality：{credibility.label}（{credibility.grade}）",
        (
            "- Uses Vendor Historical Estimates："
            f"{_yes_no(credibility.uses_vendor_historical_estimates)}"
        ),
        (
            "- Uses Self-Archived Snapshots："
            f"{_yes_no(credibility.uses_self_archived_snapshots)}"
        ),
        f"- Minimum Feature Lag：{credibility.minimum_feature_lag_days} 个交易日",
        f"- Universe PIT：{_yes_no(credibility.universe_pit)}",
        "",
        "| 输入 | 覆盖率 | point_in_time_class | backtest_use |",
        "|---|---:|---|---|",
    ]
    for item in credibility.core_inputs:
        lines.append(
            "| "
            f"{item.input_name} | "
            f"{item.coverage:.0%} | "
            f"{_counts(item.point_in_time_class_counts)} | "
            f"{_counts(item.backtest_use_counts)} |"
        )
    return lines


def _point_in_time_section(report: BacktestAuditReport) -> list[str]:
    result = report.result
    sec_feature_range = _range(
        result.fundamental_feature_row_count_min,
        result.fundamental_feature_row_count_max,
    )
    valuation_snapshot_range = _range(
        result.valuation_snapshot_count_min,
        result.valuation_snapshot_count_max,
    )
    risk_event_range = _range(
        result.risk_event_occurrence_count_min,
        result.risk_event_occurrence_count_max,
    )
    return [
        "",
        "## Point-in-Time 输入",
        "",
        "| 输入 | 切片数 | 状态统计 | 数量范围 | 警告数 |",
        "|---|---:|---|---|---:|",
        (
            f"| SEC 基本面特征 | {result.fundamental_feature_report_count} | "
            f"{_status_counts(result.fundamental_feature_status_counts)} | "
            f"{sec_feature_range} | "
            f"{result.fundamental_feature_warning_count} |"
        ),
        (
            f"| 估值快照 | {result.valuation_review_report_count} | "
            f"{_status_counts(result.valuation_review_status_counts)} | "
            f"{valuation_snapshot_range} | "
            f"{result.valuation_review_warning_count} |"
        ),
        (
            f"| 风险事件发生记录 | {result.risk_event_occurrence_review_report_count} | "
            f"{_status_counts(result.risk_event_occurrence_status_counts)} | "
            f"{risk_event_range} | "
            f"{result.risk_event_occurrence_warning_count} |"
        ),
    ]


def _component_coverage_section(report: BacktestAuditReport) -> list[str]:
    rows = []
    for component, values in sorted(_component_coverage_values(report.result).items()):
        rows.append(
            "| "
            f"{_component_label(component)} | "
            f"{len(values)} | "
            f"{sum(values) / len(values):.0%} | "
            f"{min(values):.0%} | "
            f"{max(values):.0%} |"
        )
    return [
        "",
        "## 模块覆盖率",
        "",
        f"- 审计阈值：{report.minimum_component_coverage:.0%}",
        "",
        "| 模块 | 样本数 | 平均覆盖率 | 最低覆盖率 | 最高覆盖率 |",
        "|---|---:|---:|---:|---:|",
        *rows,
    ]


def _source_type_section(report: BacktestAuditReport) -> list[str]:
    rows = [
        (
            "| "
            f"{_component_label(component)} | "
            f"{_source_type_label(source_type)} | "
            f"{count} |"
        )
        for (component, source_type), count in sorted(
            _component_source_type_counts(report.result).items()
        )
    ]
    if not rows:
        rows = ["| 无 | 无 | 0 |"]
    return [
        "",
        "## 来源类型",
        "",
        "| 模块 | Source Type | Signal Dates |",
        "|---|---|---:|",
        *rows,
    ]


def _execution_section(report: BacktestAuditReport) -> list[str]:
    result = report.result
    commission_cost = sum(row.commission_cost for row in result.rows)
    slippage_cost = sum(row.slippage_cost for row in result.rows)
    transaction_cost = sum(row.transaction_cost for row in result.rows)
    turnover = sum(row.turnover for row in result.rows)
    return [
        "",
        "## 执行假设",
        "",
        f"- 单边交易成本：{result.cost_bps:.1f} bps",
        f"- 线性滑点/盘口冲击估算：{result.slippage_bps:.1f} bps",
        f"- 累计换手：{turnover:.1f}",
        f"- 单边交易成本扣减：{commission_cost:.2%}",
        f"- 线性滑点扣减：{slippage_cost:.2%}",
        f"- 总执行成本扣减：{transaction_cost:.2%}",
        "- 尚未建模税费、汇率、融资利率、非线性盘口冲击、容量约束和盘中执行偏差。",
    ]


def _issue_section(report: BacktestAuditReport) -> list[str]:
    lines = ["", "## 审计发现", ""]
    if not report.issues:
        lines.append("未发现阻碍解释本次回测的输入审计问题。")
        return lines

    lines.extend(
        [
            "| 级别 | Code | 对象 | 说明 | 建议 |",
            "|---|---|---|---|---|",
        ]
    )
    for issue in report.issues:
        lines.append(
            "| "
            f"{_severity_label(issue.severity)} | "
            f"{issue.code} | "
            f"{_escape_markdown(issue.subject)} | "
            f"{_escape_markdown(issue.message)} | "
            f"{_escape_markdown(issue.recommendation)} |"
        )
    return lines


def _recommended_action_section(report: BacktestAuditReport) -> list[str]:
    if report.status == "PASS":
        action = "本次回测输入链路可解释；后续重点是补充正式数据源和执行模型。"
    elif report.error_count:
        action = "先修复 ERROR 项，重跑回测和审计报告后再解释策略结论。"
    else:
        action = "本次回测可以用于工程复核，但投资解释必须逐项说明 WARNING 的影响。"
    return [
        "",
        "## 结论",
        "",
        action,
    ]


def _component_coverage_values(result: DailyBacktestResult) -> dict[str, list[float]]:
    values_by_component: dict[str, list[float]] = {}
    for row in result.rows:
        for component, coverage in row.component_coverages.items():
            values_by_component.setdefault(component, []).append(coverage)
    return values_by_component


def _component_source_type_counts(
    result: DailyBacktestResult,
) -> Counter[tuple[str, str]]:
    counts: Counter[tuple[str, str]] = Counter()
    for row in result.rows:
        for component, source_type in row.component_source_types.items():
            counts[(component, source_type)] += 1
    return counts


def _source_type_count(
    counts: dict[tuple[str, str], int] | None,
    source_type: str,
) -> int:
    if not counts:
        return 0
    return sum(
        count
        for (_, item_source_type), count in counts.items()
        if item_source_type == source_type
    )


def _component_label(component: str) -> str:
    label = COMPONENT_LABELS.get(component, component)
    return f"{label}（{component}）"


def _source_type_label(source_type: str) -> str:
    label = SOURCE_TYPE_LABELS.get(source_type, source_type)
    return f"{label}（{source_type}）"


def _status_counts(status_counts: dict[str, int] | None) -> str:
    if not status_counts:
        return "无"
    return "；".join(f"{status}={count}" for status, count in sorted(status_counts.items()))


def _yes_no(value: bool) -> str:
    return "是" if value else "否"


def _counts(counts: dict[str, int]) -> str:
    if not counts:
        return "无"
    return "；".join(f"{key}={count}" for key, count in sorted(counts.items()))


def _range(min_value: int | None, max_value: int | None) -> str:
    if min_value is None or max_value is None:
        return "n/a"
    if min_value == max_value:
        return str(min_value)
    return f"{min_value}-{max_value}"


def _severity_label(severity: AuditSeverity) -> str:
    if severity == "ERROR":
        return "错误"
    return "警告"


def _escape_markdown(value: str) -> str:
    return value.replace("\n", " ").replace("|", "\\|")
