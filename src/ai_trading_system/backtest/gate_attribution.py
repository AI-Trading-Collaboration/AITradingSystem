from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

import pandas as pd

GateAttributionSeverity = Literal["ERROR", "WARNING"]

GATE_TRIGGER_SUFFIX = "_gate_triggered"
GATE_CAP_SUFFIX = "_gate_cap"


@dataclass(frozen=True)
class GateAttributionIssue:
    severity: GateAttributionSeverity
    code: str
    message: str
    recommendation: str


@dataclass(frozen=True)
class GateAttributionRow:
    gate_id: str
    trigger_count: int
    average_cap: float | None
    average_position_reduction: float
    avoided_drawdown: float
    missed_upside: float
    net_effect: float
    false_alarm_rate: float | None
    late_trigger_rate: float | None
    average_next_asset_return: float | None


@dataclass(frozen=True)
class EventAttributionSummary:
    risk_event_record_count: int
    risk_event_occurrence_count: int
    score_eligible_count: int
    source_type_counts: dict[str, int]
    status_counts: dict[str, int]
    manual_review_pass_rate: float | None
    pending_to_confirmed_rate: float | None
    event_precision: float | None
    severity_accuracy: float | None
    label_availability: str


@dataclass(frozen=True)
class GateEventAttributionReport:
    backtest_daily_path: Path
    input_coverage_path: Path | None
    as_of: date
    start_date: date | None
    end_date: date | None
    row_count: int
    left_tail_threshold: float
    gate_rows: tuple[GateAttributionRow, ...]
    event_summary: EventAttributionSummary
    issues: tuple[GateAttributionIssue, ...]

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "WARNING")

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"


def build_gate_event_attribution_report(
    *,
    backtest_daily_path: Path | str,
    input_coverage_path: Path | str | None = None,
    as_of: date,
    left_tail_threshold: float = -0.03,
) -> GateEventAttributionReport:
    daily_path = Path(backtest_daily_path)
    coverage_path = Path(input_coverage_path) if input_coverage_path else None
    issues: list[GateAttributionIssue] = []
    daily = _read_csv(daily_path, issues, "backtest_daily")
    if daily is None:
        return _failed_report(daily_path, coverage_path, as_of, left_tail_threshold, issues)

    _append_daily_schema_issues(issues, daily)
    if any(issue.severity == "ERROR" for issue in issues):
        return _failed_report(daily_path, coverage_path, as_of, left_tail_threshold, issues)

    gate_rows = _build_gate_rows(daily, left_tail_threshold, issues)
    event_summary = _build_event_summary(coverage_path, issues)
    if not gate_rows:
        _issue(
            issues,
            "WARNING",
            "no_position_gate_columns",
            "backtest daily CSV 未发现 `*_gate_triggered` gate 列。",
            "先用当前 `aits backtest` 生成包含 gate 列的 daily CSV。",
        )
    if event_summary.label_availability != "available":
        _issue(
            issues,
            "WARNING",
            "event_label_metrics_limited",
            "事件 precision、severity accuracy 和多 horizon 不利波动标签当前不可用。",
            "后续用已复核 occurrence lifecycle、prediction outcome 或事件标签账本补齐。",
        )

    start_date, end_date = _date_bounds(daily)
    return GateEventAttributionReport(
        backtest_daily_path=daily_path,
        input_coverage_path=coverage_path,
        as_of=as_of,
        start_date=start_date,
        end_date=end_date,
        row_count=len(daily),
        left_tail_threshold=left_tail_threshold,
        gate_rows=tuple(gate_rows),
        event_summary=event_summary,
        issues=tuple(issues),
    )


def render_gate_event_attribution_report(report: GateEventAttributionReport) -> str:
    lines = [
        "# Gate 与事件效果归因报告",
        "",
        f"- 状态：{report.status}",
        f"- 生成日期：{report.as_of.isoformat()}",
        f"- 回测窗口：{_date_text(report.start_date)} 至 {_date_text(report.end_date)}",
        f"- Backtest daily：`{report.backtest_daily_path}`",
        f"- Input coverage：`{report.input_coverage_path}`"
        if report.input_coverage_path
        else "- Input coverage：未提供",
        f"- 样本行数：{report.row_count}",
        f"- 左尾阈值：{report.left_tail_threshold:.1%}",
        "- production_effect：none",
        "- 边界：本报告只解释 gate/event 的历史样本表现，不改变回测、评分、"
        "仓位闸门或 execution policy。",
        "",
        "## Gate Attribution",
        "",
    ]
    if report.gate_rows:
        lines.extend(
            [
                "| Gate | Trigger count | Avg cap | Avg reduction | Avoided drawdown | "
                "Missed upside | Net effect | False alarm | Late trigger | "
                "Avg next return |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in report.gate_rows:
            lines.append(
                "| "
                f"`{row.gate_id}` | {row.trigger_count} | {_pct(row.average_cap)} | "
                f"{_pct(row.average_position_reduction)} | {_pct(row.avoided_drawdown)} | "
                f"{_pct(row.missed_upside)} | {_pct(row.net_effect)} | "
                f"{_pct(row.false_alarm_rate)} | {_pct(row.late_trigger_rate)} | "
                f"{_pct(row.average_next_asset_return)} |"
            )
    else:
        lines.append("未发现可归因的 gate 样本。")
    lines.extend(_event_summary_lines(report.event_summary))
    lines.extend(
        [
            "",
            "## 校验与限制",
            "",
        ]
    )
    if report.issues:
        lines.extend(
            [
                "| Severity | Code | 说明 | 建议 |",
                "|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                f"| {issue.severity} | `{issue.code}` | "
                f"{issue.message} | {issue.recommendation} |"
            )
    else:
        lines.append("未发现错误或限制。")
    lines.extend(
        [
            "",
            "## 解释口径",
            "",
            "- `avoided_drawdown` 以 gate 单独 cap 相对模型目标仓位的降幅乘以下一"
            "交易日负收益估算，是一阶归因，不是完整反事实回测。",
            "- `missed_upside` 用同一降幅乘以下一交易日正收益估算，"
            "便于和 avoided drawdown 对称比较。",
            "- 多 gate 同时触发时，本报告按每个 gate 的单独 cap 估算，"
            "不把净效应相加为组合收益结论。",
            "- Event / LLM 指标只在 coverage 或标签账本包含已复核事件与 outcome "
            "标签时可解释；缺标签时保持 `LIMITED`。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_gate_event_attribution_report(
    report: GateEventAttributionReport,
    output_path: Path | str,
) -> Path:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_gate_event_attribution_report(report), encoding="utf-8")
    return destination


def default_gate_event_attribution_report_path(
    output_dir: Path,
    report: GateEventAttributionReport,
) -> Path:
    start = _date_text(report.start_date)
    end = _date_text(report.end_date)
    return output_dir / f"gate_event_attribution_{start}_{end}.md"


def infer_input_coverage_path(backtest_daily_path: Path | str) -> Path | None:
    path = Path(backtest_daily_path)
    inferred_name = path.name.replace("backtest_daily_", "backtest_input_coverage_")
    if inferred_name == path.name:
        return None
    inferred = path.with_name(inferred_name)
    return inferred if inferred.exists() else None


def _read_csv(
    path: Path,
    issues: list[GateAttributionIssue],
    label: str,
) -> pd.DataFrame | None:
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        _issue(
            issues,
            "ERROR",
            f"{label}_not_found",
            f"{label} CSV 不存在：{path}",
            "先生成对应回测输出，或显式传入正确路径。",
        )
    except pd.errors.ParserError as exc:
        _issue(
            issues,
            "ERROR",
            f"{label}_parse_error",
            f"{label} CSV 解析失败：{exc}",
            "检查 CSV schema 和分隔符。",
        )
    return None


def _failed_report(
    daily_path: Path,
    coverage_path: Path | None,
    as_of: date,
    left_tail_threshold: float,
    issues: list[GateAttributionIssue],
) -> GateEventAttributionReport:
    return GateEventAttributionReport(
        backtest_daily_path=daily_path,
        input_coverage_path=coverage_path,
        as_of=as_of,
        start_date=None,
        end_date=None,
        row_count=0,
        left_tail_threshold=left_tail_threshold,
        gate_rows=(),
        event_summary=_empty_event_summary(),
        issues=tuple(issues),
    )


def _append_daily_schema_issues(
    issues: list[GateAttributionIssue],
    daily: pd.DataFrame,
) -> None:
    required = ("signal_date", "asset_return", "model_target_exposure")
    for column in required:
        if column not in daily.columns:
            _issue(
                issues,
                "ERROR",
                "missing_backtest_daily_column",
                f"backtest daily CSV 缺少 `{column}`。",
                "重新运行当前版本 `aits backtest` 生成完整 daily CSV。",
            )


def _build_gate_rows(
    daily: pd.DataFrame,
    left_tail_threshold: float,
    issues: list[GateAttributionIssue],
) -> list[GateAttributionRow]:
    asset_return = pd.to_numeric(daily["asset_return"], errors="coerce").fillna(0.0)
    model_target = pd.to_numeric(daily["model_target_exposure"], errors="coerce").fillna(0.0)
    left_tail = asset_return <= left_tail_threshold
    left_tail_count = int(left_tail.sum())
    gate_rows: list[GateAttributionRow] = []
    for trigger_column in sorted(
        column for column in daily.columns if column.endswith(GATE_TRIGGER_SUFFIX)
    ):
        gate_id = trigger_column[: -len(GATE_TRIGGER_SUFFIX)]
        cap_column = f"{gate_id}{GATE_CAP_SUFFIX}"
        if cap_column not in daily.columns:
            _issue(
                issues,
                "WARNING",
                "missing_gate_cap_column",
                f"`{trigger_column}` 没有对应 `{cap_column}`。",
                "重新运行当前版本回测，或补齐 gate cap 列。",
            )
            cap = pd.Series([1.0] * len(daily), index=daily.index)
        else:
            cap = pd.to_numeric(daily[cap_column], errors="coerce").fillna(1.0)
        triggered = _bool_series(daily[trigger_column])
        standalone_target = pd.concat([model_target, cap], axis=1).min(axis=1)
        reduction = (model_target - standalone_target).clip(lower=0.0)
        triggered_reduction = reduction.where(triggered, 0.0)
        trigger_count = int(triggered.sum())
        if trigger_count:
            average_position_reduction = float(reduction[triggered].mean())
            average_cap = float(cap[triggered].mean())
            average_next_asset_return = float(asset_return[triggered].mean())
            false_alarm_rate = float(((asset_return > 0) & triggered).sum() / trigger_count)
        else:
            average_position_reduction = 0.0
            average_cap = None
            average_next_asset_return = None
            false_alarm_rate = None
        avoided_drawdown = float((triggered_reduction * (-asset_return.clip(upper=0.0))).sum())
        missed_upside = float((triggered_reduction * asset_return.clip(lower=0.0)).sum())
        late_trigger_rate = (
            float((left_tail & ~triggered).sum() / left_tail_count)
            if left_tail_count
            else None
        )
        gate_rows.append(
            GateAttributionRow(
                gate_id=gate_id,
                trigger_count=trigger_count,
                average_cap=average_cap,
                average_position_reduction=average_position_reduction,
                avoided_drawdown=avoided_drawdown,
                missed_upside=missed_upside,
                net_effect=avoided_drawdown - missed_upside,
                false_alarm_rate=false_alarm_rate,
                late_trigger_rate=late_trigger_rate,
                average_next_asset_return=average_next_asset_return,
            )
        )
    return gate_rows


def _build_event_summary(
    coverage_path: Path | None,
    issues: list[GateAttributionIssue],
) -> EventAttributionSummary:
    if coverage_path is None:
        return _empty_event_summary(label_availability="coverage_missing")
    coverage = _read_csv(coverage_path, issues, "backtest_input_coverage")
    if coverage is None:
        return _empty_event_summary(label_availability="coverage_missing")
    record_type = (
        coverage["record_type"]
        if "record_type" in coverage.columns
        else pd.Series([""] * len(coverage), index=coverage.index)
    )
    risk_rows = coverage[
        record_type.isin({"risk_event_evidence_url", "risk_event_source_type"})
    ]
    if risk_rows.empty:
        return _empty_event_summary(label_availability="no_risk_event_rows")
    occurrence_id = (
        risk_rows["occurrence_id"]
        if "occurrence_id" in risk_rows.columns
        else pd.Series([""] * len(risk_rows), index=risk_rows.index)
    )
    occurrence_rows = risk_rows[occurrence_id.fillna("") != ""]
    occurrence_count = int(occurrence_rows["occurrence_id"].nunique())
    score_eligible_count = int(_bool_series(risk_rows.get("score_eligible", False)).sum())
    source_type_counts = _value_counts(risk_rows.get("source_type"))
    status_counts = _value_counts(risk_rows.get("status"))
    manual_review_pass_rate = (
        score_eligible_count / occurrence_count if occurrence_count else None
    )
    confirmed_count = sum(
        count
        for status, count in status_counts.items()
        if str(status).startswith("confirmed") or status == "active"
    )
    pending_count = sum(
        count
        for status, count in status_counts.items()
        if str(status).startswith("pending") or status == "watch"
    )
    pending_to_confirmed_rate = (
        confirmed_count / (confirmed_count + pending_count)
        if confirmed_count + pending_count
        else None
    )
    return EventAttributionSummary(
        risk_event_record_count=len(risk_rows),
        risk_event_occurrence_count=occurrence_count,
        score_eligible_count=score_eligible_count,
        source_type_counts=source_type_counts,
        status_counts=status_counts,
        manual_review_pass_rate=manual_review_pass_rate,
        pending_to_confirmed_rate=pending_to_confirmed_rate,
        event_precision=None,
        severity_accuracy=None,
        label_availability="limited",
    )


def _empty_event_summary(label_availability: str = "missing") -> EventAttributionSummary:
    return EventAttributionSummary(
        risk_event_record_count=0,
        risk_event_occurrence_count=0,
        score_eligible_count=0,
        source_type_counts={},
        status_counts={},
        manual_review_pass_rate=None,
        pending_to_confirmed_rate=None,
        event_precision=None,
        severity_accuracy=None,
        label_availability=label_availability,
    )


def _event_summary_lines(summary: EventAttributionSummary) -> list[str]:
    return [
        "",
        "## Event / LLM Attribution Readiness",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Risk event records | {summary.risk_event_record_count} |",
        f"| Distinct occurrences | {summary.risk_event_occurrence_count} |",
        f"| Score eligible records | {summary.score_eligible_count} |",
        f"| Manual review pass rate | {_pct(summary.manual_review_pass_rate)} |",
        f"| Pending to confirmed rate | {_pct(summary.pending_to_confirmed_rate)} |",
        f"| Event precision | {_pct(summary.event_precision)} |",
        f"| Severity accuracy | {_pct(summary.severity_accuracy)} |",
        f"| Label availability | `{summary.label_availability}` |",
        "",
        "- Source type counts: " + _counts(summary.source_type_counts),
        "- Status counts: " + _counts(summary.status_counts),
    ]


def _bool_series(value: pd.Series | bool) -> pd.Series:
    if isinstance(value, bool):
        return pd.Series([value])
    if value.dtype == bool:
        return value.fillna(False)
    return value.astype(str).str.lower().isin({"true", "1", "yes", "y", "是"})


def _value_counts(value: pd.Series | None) -> dict[str, int]:
    if value is None:
        return {}
    counter: Counter[str] = Counter()
    for item in value.fillna("").astype(str):
        if item.strip():
            counter[item.strip()] += 1
    return dict(sorted(counter.items()))


def _date_bounds(daily: pd.DataFrame) -> tuple[date | None, date | None]:
    parsed = pd.to_datetime(daily["signal_date"], errors="coerce")
    valid = parsed.dropna()
    if valid.empty:
        return None, None
    return valid.min().date(), valid.max().date()


def _date_text(value: date | None) -> str:
    return value.isoformat() if value else "unknown"


def _pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.2%}"


def _counts(counts: dict[str, int]) -> str:
    if not counts:
        return "无"
    return "；".join(f"{key}={count}" for key, count in sorted(counts.items()))


def _issue(
    issues: list[GateAttributionIssue],
    severity: GateAttributionSeverity,
    code: str,
    message: str,
    recommendation: str,
) -> None:
    issues.append(
        GateAttributionIssue(
            severity=severity,
            code=code,
            message=message,
            recommendation=recommendation,
        )
    )
