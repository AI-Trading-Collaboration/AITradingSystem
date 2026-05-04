from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

from ai_trading_system.catalyst_calendar import CatalystCalendarValidationReport
from ai_trading_system.pipeline_health import (
    PipelineHealthReport,
    PipelineHealthSeverity,
)
from ai_trading_system.scoring.daily import DailyScoreReport, PreviousDailyScoreSnapshot

AlertCategory = Literal["data_system", "investment_risk"]
AlertSeverity = Literal["info", "warning", "high", "critical"]

DEFAULT_ALERT_REPORT_DIR = Path(__file__).resolve().parents[2] / "outputs" / "reports"
ROUTINE_POSITION_GATE_IDS = {"score_model", "portfolio_limits"}


@dataclass(frozen=True)
class AlertRecord:
    alert_id: str
    as_of: date
    category: AlertCategory
    severity: AlertSeverity
    source: str
    title: str
    trigger_condition: str
    clear_condition: str
    message: str
    claim_refs: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    dedupe_key: str
    production_effect: Literal["none"] = "none"


@dataclass(frozen=True)
class AlertReport:
    as_of: date
    alerts: tuple[AlertRecord, ...]

    @property
    def status(self) -> str:
        if any(alert.severity == "critical" for alert in self.alerts):
            return "ACTIVE_CRITICAL"
        if any(alert.severity in {"high", "warning"} for alert in self.alerts):
            return "ACTIVE_WARNINGS"
        return "PASS"

    @property
    def data_system_count(self) -> int:
        return sum(1 for alert in self.alerts if alert.category == "data_system")

    @property
    def investment_risk_count(self) -> int:
        return sum(1 for alert in self.alerts if alert.category == "investment_risk")

    @property
    def critical_count(self) -> int:
        return sum(1 for alert in self.alerts if alert.severity == "critical")

    @property
    def high_count(self) -> int:
        return sum(1 for alert in self.alerts if alert.severity == "high")

    @property
    def warning_count(self) -> int:
        return sum(1 for alert in self.alerts if alert.severity == "warning")


def default_alert_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"alerts_{as_of.isoformat()}.md"


def default_pipeline_health_alert_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"pipeline_health_alerts_{as_of.isoformat()}.md"


def build_pipeline_health_alert_report(
    report: PipelineHealthReport,
    *,
    pipeline_health_report_path: Path | None = None,
) -> AlertReport:
    alerts = [
        _alert(
            as_of=report.as_of,
            category="data_system",
            severity="high"
            if check.severity == PipelineHealthSeverity.ERROR
            else "warning",
            source="aits ops health",
            code=f"pipeline_health_{check.spec.artifact_id}",
            title=f"{check.spec.label} 异常",
            trigger_condition=f"health_status={check.severity.value}；{check.message}",
            clear_condition=(
                "修复输入或重新运行对应步骤后，重新执行 `aits ops health`，"
                f"`{check.spec.artifact_id}` 状态恢复 OK。"
            ),
            message=f"{check.message} 排查入口：{check.spec.investigation_hint}",
            claim_refs=(
                f"pipeline_health:{report.as_of.isoformat()}:{check.spec.artifact_id}",
            ),
            evidence_refs=_path_refs(pipeline_health_report_path, str(check.spec.path)),
        )
        for check in report.checks
        if check.severity is not None
    ]
    return AlertReport(as_of=report.as_of, alerts=tuple(sorted(alerts, key=_alert_sort_key)))


def build_daily_alert_report(
    report: DailyScoreReport,
    *,
    previous_score_snapshot: PreviousDailyScoreSnapshot | None = None,
    catalyst_calendar_report: CatalystCalendarValidationReport | None = None,
    data_quality_report_path: Path | None = None,
    risk_event_occurrence_report_path: Path | None = None,
    valuation_report_path: Path | None = None,
    catalyst_calendar_path: Path | None = None,
    position_drop_threshold: float = 0.10,
) -> AlertReport:
    alerts: list[AlertRecord] = []
    seen: set[str] = set()

    def add(alert: AlertRecord) -> None:
        if alert.dedupe_key in seen:
            return
        seen.add(alert.dedupe_key)
        alerts.append(alert)

    as_of = report.as_of
    if report.data_quality_report.status != "PASS":
        add(
            _alert(
                as_of=as_of,
                category="data_system",
                severity="high"
                if report.data_quality_report.error_count
                else "warning",
                source="aits validate-data",
                code="data_quality_not_pass",
                title="数据质量门禁未完全通过",
                trigger_condition=(
                    f"data_quality_status={report.data_quality_report.status}；"
                    f"errors={report.data_quality_report.error_count}；"
                    f"warnings={report.data_quality_report.warning_count}"
                ),
                clear_condition="`aits validate-data` 状态为 PASS 且没有 warning。",
                message="缓存市场/宏观数据存在质量问题，下游结论只能带限制使用。",
                claim_refs=(f"daily_score:{as_of.isoformat()}:data_quality",),
                evidence_refs=_path_refs(data_quality_report_path),
            )
        )

    if report.feature_set.warnings:
        add(
            _alert(
                as_of=as_of,
                category="data_system",
                severity="warning",
                source="market_features",
                code="feature_warnings_present",
                title="市场特征存在警告",
                trigger_condition=f"feature_warning_count={len(report.feature_set.warnings)}",
                clear_condition="重新生成特征后 warning_count=0。",
                message="市场特征窗口或输入覆盖存在警告，需要查看特征摘要。",
                claim_refs=(f"daily_score:{as_of.isoformat()}:features",),
                evidence_refs=(),
            )
        )

    for component in report.components:
        if component.source_type in {"placeholder", "insufficient_data"}:
            add(
                _alert(
                    as_of=as_of,
                    category="data_system",
                    severity="warning",
                    source=f"score_module:{component.name}",
                    code=f"module_{component.name}_limited",
                    title=f"{component.name} 模块输入不足",
                    trigger_condition=(
                        f"source_type={component.source_type}；"
                        f"confidence={component.confidence:.0%}"
                    ),
                    clear_condition=(
                        "模块输入切换为可审计 hard/manual data，"
                        "且置信度恢复到 60% 以上。"
                    ),
                    message=component.reason,
                    claim_refs=(f"daily_score:{as_of.isoformat()}:{component.name}",),
                    evidence_refs=(),
                )
            )

    if report.valuation_review_report is not None:
        for item in report.valuation_review_report.items:
            if item.confidence_level == "low" or (as_of - item.as_of).days > 45:
                add(
                    _alert(
                        as_of=as_of,
                        category="data_system",
                        severity="warning",
                        source="valuation",
                        code=f"valuation_{item.ticker}_stale_or_low_confidence",
                        title=f"{item.ticker} 估值输入可信度不足",
                        trigger_condition=(
                            f"confidence={item.confidence_level}；"
                            f"snapshot_as_of={item.as_of.isoformat()}；health={item.health}"
                        ),
                        clear_condition="估值快照更新且 PIT/可信度校验不再触发 low 或 stale。",
                        message=item.confidence_reason,
                        claim_refs=(f"daily_score:{as_of.isoformat()}:valuation",),
                        evidence_refs=_path_refs(valuation_report_path, item.snapshot_id),
                    )
                )
            if item.health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}:
                add(
                    _alert(
                        as_of=as_of,
                        category="investment_risk",
                        severity="high"
                        if item.health == "EXTREME_OVERHEATED"
                        else "warning",
                        source="valuation",
                        code=f"valuation_{item.ticker}_{item.health.lower()}",
                        title=f"{item.ticker} 估值拥挤",
                        trigger_condition=(
                            f"health={item.health}；"
                            f"valuation_percentile={_format_optional_number(item.valuation_percentile)}"
                        ),
                        clear_condition=(
                            "估值健康状态不再是 "
                            "EXPENSIVE_OR_CROWDED/EXTREME_OVERHEATED。"
                        ),
                        message=item.reason,
                        claim_refs=(f"daily_score:{as_of.isoformat()}:valuation_gate",),
                        evidence_refs=_path_refs(valuation_report_path, item.snapshot_id),
                    )
                )

    if report.risk_event_occurrence_review_report is not None:
        for item in report.risk_event_occurrence_review_report.position_gate_eligible_active_items:
            add(
                _alert(
                    as_of=as_of,
                    category="investment_risk",
                    severity="critical" if item.level == "L3" else "high",
                    source="risk_events",
                    code=f"risk_event_{item.occurrence_id}",
                    title=f"{item.level} 风险事件可触发仓位闸门",
                    trigger_condition=(
                        f"status={item.status}；evidence_grade={item.evidence_grade}；"
                        f"action_class={item.action_class}"
                    ),
                    clear_condition="风险事件 resolved，或人工复核确认不再具备 scoring/gate 资格。",
                    message=item.reason,
                    claim_refs=(f"daily_score:{as_of.isoformat()}:risk_events",),
                    evidence_refs=_path_refs(
                        risk_event_occurrence_report_path,
                        item.occurrence_id,
                    ),
                )
            )

    if report.review_summary and report.review_summary.thesis is not None:
        thesis = report.review_summary.thesis
        if thesis.status == "FAIL" or thesis.error_count:
            severity: AlertSeverity = "high"
        elif "WARNING" in thesis.status or thesis.warning_count:
            severity = "warning"
        else:
            severity = "info"
        if severity != "info":
            add(
                _alert(
                    as_of=as_of,
                    category="investment_risk",
                    severity=severity,
                    source="thesis",
                    code="thesis_review_attention",
                    title="交易 thesis 复核需要关注",
                    trigger_condition=(
                        f"status={thesis.status}；errors={thesis.error_count}；"
                        f"warnings={thesis.warning_count}"
                    ),
                    clear_condition="thesis 复核状态恢复 PASS，且没有 error/warning。",
                    message=thesis.summary,
                    claim_refs=(f"daily_score:{as_of.isoformat()}:thesis",),
                    evidence_refs=_path_refs(thesis.source_path),
                )
            )

    for gate in report.recommendation.triggered_position_gates:
        if gate.gate_id in ROUTINE_POSITION_GATE_IDS:
            continue
        add(
            _alert(
                as_of=as_of,
                category="investment_risk",
                severity=_gate_severity(gate.max_position),
                source="position_gate",
                code=f"position_gate_{gate.gate_id}",
                title=f"{gate.label} 触发仓位上限",
                trigger_condition=(
                    f"gate_id={gate.gate_id}；max_position={gate.max_position:.0%}"
                ),
                clear_condition="对应 gate 不再触发，或 max_position 不低于当前最终区间上限。",
                message=gate.reason,
                claim_refs=(f"daily_score:{as_of.isoformat()}:overall_position",),
                evidence_refs=(gate.source,),
            )
        )

    current_max = report.recommendation.risk_asset_ai_band.max_position
    previous_max = (
        previous_score_snapshot.final_risk_asset_ai_max
        if previous_score_snapshot is not None
        else None
    )
    if previous_max is not None and previous_max - current_max >= position_drop_threshold:
        add(
            _alert(
                as_of=as_of,
                category="investment_risk",
                severity="high",
                source="score-daily",
                code="position_cap_drop",
                title="最终 AI 仓位上限大幅下降",
                trigger_condition=(
                    f"previous_max={previous_max:.0%}；current_max={current_max:.0%}；"
                    f"drop={previous_max - current_max:.0%}"
                ),
                clear_condition=(
                    f"最终 AI 仓位上限较上一期下降幅度低于 {position_drop_threshold:.0%}。"
                ),
                message="仓位上限出现较大下调，需要复核触发 gate 和新增证据。",
                claim_refs=(f"daily_score:{as_of.isoformat()}:position_change",),
                evidence_refs=(),
            )
        )

    if catalyst_calendar_report is not None:
        if catalyst_calendar_report.status != "PASS":
            add(
                _alert(
                    as_of=as_of,
                    category="data_system",
                    severity="high"
                    if catalyst_calendar_report.error_count
                    else "warning",
                    source="catalyst_calendar",
                    code="catalyst_calendar_not_pass",
                    title="催化剂日历校验未完全通过",
                    trigger_condition=(
                        f"status={catalyst_calendar_report.status}；"
                        f"errors={catalyst_calendar_report.error_count}；"
                        f"warnings={catalyst_calendar_report.warning_count}"
                    ),
                    clear_condition="`aits catalysts validate` 状态为 PASS。",
                    message="未来事件输入存在结构或复核问题，事件前告警可能不完整。",
                    claim_refs=(f"daily_score:{as_of.isoformat()}:catalysts",),
                    evidence_refs=_path_refs(catalyst_calendar_path),
                )
            )
        for event in catalyst_calendar_report.upcoming_events(5):
            if event.importance in {"high", "critical"}:
                add(
                    _alert(
                        as_of=as_of,
                        category="investment_risk",
                        severity="critical"
                        if event.importance == "critical"
                        else "high",
                        source="catalyst_calendar",
                        code=f"catalyst_{event.catalyst_id}",
                        title="未来 5 天存在重要催化剂",
                        trigger_condition=(
                            f"event_date={event.event_date.isoformat()}；"
                            f"importance={event.importance}；status={event.status}"
                        ),
                        clear_condition="事件日期已过，且 post-event review 已完成或不再需要。",
                        message=event.title,
                        claim_refs=(f"daily_score:{as_of.isoformat()}:catalysts",),
                        evidence_refs=_path_refs(catalyst_calendar_path, event.catalyst_id),
                    )
                )

    return AlertReport(as_of=as_of, alerts=tuple(sorted(alerts, key=_alert_sort_key)))


def write_alert_report(report: AlertReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_alert_report(report), encoding="utf-8")
    return output_path


def render_alert_report(report: AlertReport) -> str:
    lines = [
        "# 投资与数据告警报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        "- production_effect=none；告警只做复核提示，不改变评分、仓位、回测或执行建议。",
        f"- 活跃告警数：{len(report.alerts)}",
        f"- data/system：{report.data_system_count}",
        f"- investment/risk：{report.investment_risk_count}",
        "",
        "## 严重度摘要",
        "",
        "| 等级 | 数量 |",
        "|---|---:|",
        f"| critical | {report.critical_count} |",
        f"| high | {report.high_count} |",
        f"| warning | {report.warning_count} |",
        "",
        "## 告警明细",
        "",
    ]
    if not report.alerts:
        lines.append("未触发告警。")
    else:
        lines.extend(_alert_table(report.alerts, include_message=True))
    lines.extend(
        [
            "",
            "## 治理边界",
            "",
            "- 第一阶段不接邮件、IM、桌面通知或后台调度。",
            "- 去重键用于后续通知抑制，当前报告不持久化确认/解除状态。",
            "- 告警不是交易指令；正式仓位仍由 `score-daily`、`position_gate` 和执行纪律决定。",
        ]
    )
    return "\n".join(lines) + "\n"


def render_alert_summary_section(
    report: AlertReport,
    *,
    report_path: Path | None = None,
) -> str:
    lines = [
        "## 告警摘要",
        "",
        f"- 告警状态：{report.status}",
        (
            f"- 活跃告警：{len(report.alerts)}；"
            f"data/system {report.data_system_count}；"
            f"investment/risk {report.investment_risk_count}"
        ),
        "- production_effect=none；本节只提示复核，不改变评分、仓位、回测或执行建议。",
    ]
    if report_path is not None:
        lines.append(f"- 独立报告：`{report_path}`")
    lines.append("")
    if not report.alerts:
        lines.append("未触发告警。")
    else:
        lines.extend(_alert_table(report.alerts[:8], include_message=False))
        if len(report.alerts) > 8:
            lines.append(f"- 仅展示前 8 条；完整告警数：{len(report.alerts)}。")
    return "\n".join(lines)


def _alert(
    *,
    as_of: date,
    category: AlertCategory,
    severity: AlertSeverity,
    source: str,
    code: str,
    title: str,
    trigger_condition: str,
    clear_condition: str,
    message: str,
    claim_refs: tuple[str, ...],
    evidence_refs: tuple[str, ...],
) -> AlertRecord:
    dedupe_key = f"{category}:{source}:{code}"
    return AlertRecord(
        alert_id=f"alert:{as_of.isoformat()}:{code}",
        as_of=as_of,
        category=category,
        severity=severity,
        source=source,
        title=title,
        trigger_condition=trigger_condition,
        clear_condition=clear_condition,
        message=message,
        claim_refs=claim_refs,
        evidence_refs=evidence_refs,
        dedupe_key=dedupe_key,
    )


def _gate_severity(max_position: float) -> AlertSeverity:
    if max_position <= 0.20:
        return "critical"
    if max_position <= 0.40:
        return "high"
    return "warning"


def _path_refs(path: Path | None, *extra: str) -> tuple[str, ...]:
    refs: list[str] = []
    if path is not None:
        refs.append(str(path))
    refs.extend(item for item in extra if item)
    return tuple(refs)


def _alert_sort_key(alert: AlertRecord) -> tuple[int, str, str]:
    severity_rank = {"critical": 0, "high": 1, "warning": 2, "info": 3}
    return (severity_rank[alert.severity], alert.category, alert.alert_id)


def _format_optional_number(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}"


def _alert_table(
    alerts: tuple[AlertRecord, ...],
    *,
    include_message: bool,
) -> list[str]:
    if include_message:
        lines = [
            "| 等级 | 类别 | 来源 | 标题 | 触发条件 | 解除条件 | 引用 | 去重键 | 说明 |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    else:
        lines = [
            "| 等级 | 类别 | 来源 | 标题 | 触发条件 | 解除条件 | 引用 |",
            "|---|---|---|---|---|---|---|",
        ]
    for alert in alerts:
        refs = ", ".join((*alert.claim_refs, *alert.evidence_refs))
        row = [
            alert.severity,
            alert.category,
            alert.source,
            alert.title,
            alert.trigger_condition,
            alert.clear_condition,
            refs,
        ]
        if include_message:
            row.extend([alert.dedupe_key, alert.message])
        lines.append("| " + " | ".join(_escape_markdown_table(item) for item in row) + " |")
    return lines


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
