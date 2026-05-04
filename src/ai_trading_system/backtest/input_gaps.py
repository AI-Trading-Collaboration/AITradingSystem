from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ai_trading_system.backtest.daily import BacktestRegimeContext
from ai_trading_system.risk_events import RiskEventOccurrenceReviewReport
from ai_trading_system.valuation import ValuationReviewReport


@dataclass(frozen=True)
class BacktestInputGapRow:
    signal_date: date
    valuation_status: str
    valuation_snapshot_count: int
    strict_point_in_time_valuation_count: int
    risk_event_status: str
    risk_event_occurrence_count: int
    risk_event_attestation_count: int
    risk_event_coverage_status: str


@dataclass(frozen=True)
class BacktestInputGapReport:
    requested_start: date
    requested_end: date
    signal_dates: tuple[date, ...]
    rows: tuple[BacktestInputGapRow, ...]
    valuation_path: Path
    risk_event_occurrences_path: Path
    market_regime: BacktestRegimeContext | None = None

    @property
    def status(self) -> str:
        if not self.signal_dates:
            return "FAIL"
        if self.valuation_gap_count or self.risk_event_gap_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def valuation_gap_count(self) -> int:
        return sum(1 for row in self.rows if row.valuation_snapshot_count == 0)

    @property
    def valuation_non_strict_count(self) -> int:
        return sum(
            1
            for row in self.rows
            if row.valuation_snapshot_count > 0
            and row.strict_point_in_time_valuation_count == 0
        )

    @property
    def risk_event_gap_count(self) -> int:
        return sum(
            1
            for row in self.rows
            if row.risk_event_coverage_status == "missing_or_not_reviewed"
        )


def build_backtest_input_gap_report(
    *,
    signal_dates: tuple[date, ...],
    requested_start: date,
    requested_end: date,
    valuation_reports: dict[date, ValuationReviewReport],
    risk_event_reports: dict[date, RiskEventOccurrenceReviewReport],
    valuation_path: Path,
    risk_event_occurrences_path: Path,
    market_regime: BacktestRegimeContext | None = None,
) -> BacktestInputGapReport:
    rows: list[BacktestInputGapRow] = []
    for signal_date in signal_dates:
        valuation_report = valuation_reports[signal_date]
        risk_event_report = risk_event_reports[signal_date]
        rows.append(
            BacktestInputGapRow(
                signal_date=signal_date,
                valuation_status=valuation_report.status,
                valuation_snapshot_count=valuation_report.validation_report.snapshot_count,
                strict_point_in_time_valuation_count=sum(
                    1
                    for item in valuation_report.items
                    if item.backtest_use == "strict_point_in_time"
                ),
                risk_event_status=risk_event_report.status,
                risk_event_occurrence_count=(
                    risk_event_report.validation_report.occurrence_count
                ),
                risk_event_attestation_count=(
                    risk_event_report.validation_report.current_review_attestation_count
                ),
                risk_event_coverage_status=_risk_event_coverage_status(
                    risk_event_report
                ),
            )
        )
    return BacktestInputGapReport(
        requested_start=requested_start,
        requested_end=requested_end,
        signal_dates=signal_dates,
        rows=tuple(rows),
        valuation_path=valuation_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        market_regime=market_regime,
    )


def render_backtest_input_gap_report(report: BacktestInputGapReport) -> str:
    status_counts = Counter(row.risk_event_coverage_status for row in report.rows)
    lines = [
        "# 回测历史输入缺口报告",
        "",
        f"- 状态：{report.status}",
        f"- 请求区间：{report.requested_start.isoformat()} 至 {report.requested_end.isoformat()}",
        f"- 信号日数量：{len(report.signal_dates)}",
        f"- 估值缺口信号日：{report.valuation_gap_count}",
        f"- 估值非严格 PIT 信号日：{report.valuation_non_strict_count}",
        f"- 风险事件/复核声明缺口信号日：{report.risk_event_gap_count}",
        f"- 估值输入路径：`{report.valuation_path}`",
        f"- 风险事件发生记录路径：`{report.risk_event_occurrences_path}`",
    ]
    if report.market_regime is not None:
        lines.extend(
            [
                f"- 市场阶段：{report.market_regime.name}（{report.market_regime.regime_id}）",
                f"- 阶段默认起点：{report.market_regime.start_date.isoformat()}",
            ]
        )

    lines.extend(
        [
            "",
            "## 方法边界",
            "",
            (
                "本报告只诊断历史输入覆盖，不生成或回填任何估值、盈利预期、风险事件"
                "或“无风险”声明。"
            ),
            (
                "`captured_at_forward_only` 或回填历史估值分布只能按采集日之后可见解释，"
                "不得伪装成严格 point-in-time vendor archive。"
            ),
            (
                "风险事件发生记录为 0 不能自动解释为历史无事件；只有当 signal_date "
                "存在当前有效人工复核声明时，才可把空发生记录解释为已复核覆盖。"
            ),
            "",
            "## 覆盖摘要",
            "",
            "| 项目 | 数量 |",
            "|---|---:|",
            f"| signal_date | {len(report.signal_dates)} |",
            f"| 有估值快照 | {len(report.rows) - report.valuation_gap_count} |",
            f"| 有严格 PIT 估值 | {_strict_pit_signal_count(report)} |",
            (
                "| 风险事件 occurrence 覆盖 | "
                f"{status_counts.get('covered_by_occurrence', 0)} |"
            ),
            (
                "| 人工复核声明覆盖空事件 | "
                f"{status_counts.get('covered_by_review_attestation', 0)} |"
            ),
            (
                "| 风险事件缺口或未复核 | "
                f"{status_counts.get('missing_or_not_reviewed', 0)} |"
            ),
            "",
            "## 信号日明细",
            "",
            (
                "| Signal Date | 估值状态 | 估值快照 | 严格 PIT 估值 | "
                "风险状态 | Occurrence | 复核声明 | 风险覆盖 |"
            ),
            "|---|---|---:|---:|---|---:|---:|---|",
        ]
    )
    for row in report.rows:
        lines.append(
            "| "
            f"{row.signal_date.isoformat()} | "
            f"{row.valuation_status} | "
            f"{row.valuation_snapshot_count} | "
            f"{row.strict_point_in_time_valuation_count} | "
            f"{row.risk_event_status} | "
            f"{row.risk_event_occurrence_count} | "
            f"{row.risk_event_attestation_count} | "
            f"{row.risk_event_coverage_status} |"
        )

    lines.extend(
        [
            "",
            "## 补数入口",
            "",
            "- 估值快照模板：`docs/examples/valuation_snapshots/nvda_valuation_template.yaml`",
            (
                "- 风险事件 occurrence 模板："
                "`docs/examples/risk_event_occurrences/export_control_active_template.yaml`"
            ),
            (
                "- 风险事件复核声明模板："
                "`docs/examples/risk_event_occurrences/review_attestation_template.yaml`"
            ),
            "- 估值 CSV 导入：`aits valuation import-csv`",
            "- 风险事件 CSV 导入：`aits risk-events import-occurrences-csv`",
        ]
    )
    return "\n".join(lines) + "\n"


def write_backtest_input_gap_report(
    report: BacktestInputGapReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_backtest_input_gap_report(report), encoding="utf-8")
    return output_path


def default_backtest_input_gap_report_path(
    output_dir: Path,
    start: date,
    end: date,
) -> Path:
    return output_dir / f"backtest_input_gaps_{start.isoformat()}_{end.isoformat()}.md"


def _risk_event_coverage_status(report: RiskEventOccurrenceReviewReport) -> str:
    if report.validation_report.occurrence_count > 0:
        return "covered_by_occurrence"
    if report.has_current_review_attestation:
        return "covered_by_review_attestation"
    return "missing_or_not_reviewed"


def _strict_pit_signal_count(report: BacktestInputGapReport) -> int:
    return sum(1 for row in report.rows if row.strict_point_in_time_valuation_count > 0)
