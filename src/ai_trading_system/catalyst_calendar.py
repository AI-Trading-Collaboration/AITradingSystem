from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import (
    DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    IndustryChainConfig,
    RiskEventsConfig,
    WatchlistConfig,
)

CalendarStatus = Literal["baseline_empty", "active", "retired"]
CatalystStatus = Literal["scheduled", "tentative", "completed", "cancelled"]
CatalystType = Literal[
    "earnings",
    "guidance",
    "macro",
    "capex",
    "product_launch",
    "regulatory",
    "export_control",
    "industry_conference",
    "other",
]
CatalystImportance = Literal["low", "medium", "high", "critical"]
CatalystSourceType = Literal[
    "official",
    "paid_vendor",
    "manual_input",
    "public_convenience",
]
PreEventAction = Literal[
    "pre_event_review",
    "no_new_position",
    "conclusion_downgrade",
    "manual_review_only",
    "none",
]
PostEventReviewTarget = Literal[
    "thesis",
    "risk_event",
    "valuation",
    "scenario",
    "data_quality",
]

IMPORTANCE_ORDER = {"low": 1, "medium": 2, "high": 3, "critical": 4}


class CatalystCalendarIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class CatalystEvent(BaseModel):
    catalyst_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    title: str = Field(min_length=1)
    event_type: CatalystType
    event_date: date
    status: CatalystStatus
    importance: CatalystImportance
    related_tickers: list[str] = Field(default_factory=list)
    related_nodes: list[str] = Field(default_factory=list)
    linked_thesis_ids: list[str] = Field(default_factory=list)
    linked_risk_event_ids: list[str] = Field(default_factory=list)
    pre_event_actions: list[PreEventAction] = Field(default_factory=list)
    post_event_review_targets: list[PostEventReviewTarget] = Field(default_factory=list)
    source_name: str = Field(min_length=1)
    source_type: CatalystSourceType
    source_url: str = ""
    captured_at: datetime
    reviewer: str = Field(min_length=1)
    reviewed_at: date
    confidence: Literal["low", "medium", "high"]
    notes: str = ""

    @model_validator(mode="after")
    def validate_mapping_presence(self) -> Self:
        if not self.related_tickers and not self.related_nodes:
            raise ValueError("catalyst requires related_tickers or related_nodes")
        return self


class CatalystCalendar(BaseModel):
    calendar_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1, pattern=r"^v[0-9]+([.][0-9]+)*$")
    status: CalendarStatus
    owner: str = Field(min_length=1)
    description: str = Field(min_length=1)
    source_policy: str = Field(min_length=1)
    last_reviewed_at: date
    next_review_due: date
    events: list[CatalystEvent] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_review_dates(self) -> Self:
        if self.next_review_due < self.last_reviewed_at:
            raise ValueError("next_review_due must not be before last_reviewed_at")
        return self


@dataclass(frozen=True)
class CatalystCalendarIssue:
    severity: CatalystCalendarIssueSeverity
    code: str
    message: str
    catalyst_id: str | None = None


@dataclass(frozen=True)
class CatalystCalendarStore:
    input_path: Path
    calendar: CatalystCalendar | None
    load_errors: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class CatalystCalendarValidationReport:
    as_of: date
    store: CatalystCalendarStore
    issues: tuple[CatalystCalendarIssue, ...]
    windows: tuple[int, ...] = (5, 20, 60)

    @property
    def event_count(self) -> int:
        if self.store.calendar is None:
            return 0
        return len(self.store.calendar.events)

    @property
    def upcoming_count(self) -> int:
        return len(self.upcoming_events(max(self.windows)))

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == CatalystCalendarIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == CatalystCalendarIssueSeverity.WARNING
        )

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"

    def upcoming_events(self, horizon_days: int) -> tuple[CatalystEvent, ...]:
        if self.store.calendar is None:
            return ()
        end_date = self.as_of + timedelta(days=horizon_days)
        return tuple(
            sorted(
                (
                    event
                    for event in self.store.calendar.events
                    if self.as_of <= event.event_date <= end_date
                    and event.status in {"scheduled", "tentative"}
                ),
                key=lambda item: (item.event_date, -IMPORTANCE_ORDER[item.importance]),
            )
        )


def load_catalyst_calendar(
    input_path: Path | str = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
) -> CatalystCalendarStore:
    path = Path(input_path)
    if not path.exists():
        return CatalystCalendarStore(
            input_path=path,
            calendar=None,
            load_errors=(f"文件不存在：{path}",),
        )
    try:
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
    except (OSError, yaml.YAMLError) as exc:
        return CatalystCalendarStore(input_path=path, calendar=None, load_errors=(str(exc),))

    try:
        calendar = CatalystCalendar.model_validate(raw)
    except ValidationError as exc:
        return CatalystCalendarStore(
            input_path=path,
            calendar=None,
            load_errors=(_compact_validation_error(exc),),
        )
    return CatalystCalendarStore(input_path=path, calendar=calendar)


def validate_catalyst_calendar(
    store: CatalystCalendarStore,
    *,
    as_of: date,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    risk_events: RiskEventsConfig,
    windows: tuple[int, ...] = (5, 20, 60),
) -> CatalystCalendarValidationReport:
    issues: list[CatalystCalendarIssue] = []
    for error in store.load_errors:
        issues.append(
            CatalystCalendarIssue(
                severity=CatalystCalendarIssueSeverity.ERROR,
                code="catalyst_calendar_load_error",
                message=error,
            )
        )
    calendar = store.calendar
    if calendar is not None:
        _check_duplicate_catalyst_ids(calendar, issues)
        _check_calendar_review_due(calendar, as_of, issues)
        if calendar.status == "active" and not calendar.events:
            issues.append(
                CatalystCalendarIssue(
                    severity=CatalystCalendarIssueSeverity.WARNING,
                    code="active_calendar_has_no_events",
                    message="active catalyst calendar 没有事件记录。",
                )
            )
        known_nodes = {node.node_id for node in industry_chain.nodes}
        known_tickers = _known_tickers(industry_chain, watchlist, risk_events)
        known_risk_events = {event.event_id for event in risk_events.event_rules}
        for event in calendar.events:
            _check_event(
                event,
                as_of=as_of,
                known_nodes=known_nodes,
                known_tickers=known_tickers,
                known_risk_events=known_risk_events,
                issues=issues,
            )
    return CatalystCalendarValidationReport(
        as_of=as_of,
        store=store,
        issues=tuple(issues),
        windows=tuple(sorted(set(windows))),
    )


def default_catalyst_calendar_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"catalyst_calendar_{as_of.isoformat()}.md"


def write_catalyst_calendar_report(
    report: CatalystCalendarValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_catalyst_calendar_report(report), encoding="utf-8")
    return output_path


def lookup_catalyst(input_path: Path | str, catalyst_id: str) -> CatalystEvent:
    store = load_catalyst_calendar(input_path)
    if store.load_errors:
        raise ValueError("; ".join(store.load_errors))
    if store.calendar is None:
        raise KeyError(f"catalyst calendar not found: {input_path}")
    for event in store.calendar.events:
        if event.catalyst_id == catalyst_id:
            return event
    raise KeyError(f"catalyst not found: {catalyst_id}")


def render_catalyst_lookup(event: CatalystEvent) -> str:
    lines = [
        f"Catalyst：{event.catalyst_id}",
        f"标题：{event.title}",
        f"日期：{event.event_date.isoformat()}",
        f"类型：{event.event_type}",
        f"状态：{event.status}",
        f"重要性：{event.importance}",
        f"关联 ticker：{', '.join(event.related_tickers) or '无'}",
        f"关联节点：{', '.join(event.related_nodes) or '无'}",
        f"关联风险事件：{', '.join(event.linked_risk_event_ids) or '无'}",
        f"事件前动作：{', '.join(event.pre_event_actions) or '无'}",
        f"事件后复核：{', '.join(event.post_event_review_targets) or '无'}",
        f"来源：{event.source_name}（{event.source_type}）",
        f"采集时间：{event.captured_at.isoformat()}",
        f"复核：{event.reviewer} / {event.reviewed_at.isoformat()}",
        f"置信度：{event.confidence}",
        f"备注：{event.notes or '无'}",
    ]
    return "\n".join(lines) + "\n"


def render_catalyst_calendar_report(report: CatalystCalendarValidationReport) -> str:
    calendar = report.store.calendar
    lines = [
        "# 未来催化剂日历校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.store.input_path}`",
        f"- 事件数量：{report.event_count}",
        f"- 未来 {max(report.windows)} 天事件数：{report.upcoming_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "- 治理边界：催化剂日历用于事件前/后复核和报告提醒；"
        "第一阶段不直接修改 production 评分、仓位闸门或日报仓位。",
    ]
    if calendar is not None:
        type_counts = Counter(event.event_type for event in calendar.events)
        importance_counts = Counter(event.importance for event in calendar.events)
        lines.extend(
            [
                f"- Calendar：`{calendar.calendar_id}` `{calendar.version}`",
                f"- 日历状态：{calendar.status}",
                f"- Source policy：{calendar.source_policy}",
                f"- 下次复核：{calendar.next_review_due.isoformat()}",
                "",
                "## 类型摘要",
                "",
                "| 类型 | 数量 |",
                "|---|---:|",
            ]
        )
        if type_counts:
            for event_type, count in sorted(type_counts.items()):
                lines.append(f"| {event_type} | {count} |")
        else:
            lines.append("| 无事件 | 0 |")

        lines.extend(["", "## 重要性摘要", "", "| 重要性 | 数量 |", "|---|---:|"])
        if importance_counts:
            for importance, count in sorted(importance_counts.items()):
                lines.append(f"| {importance} | {count} |")
        else:
            lines.append("| 无事件 | 0 |")

        lines.extend(["", "## 未来窗口", ""])
        for window in report.windows:
            lines.extend([f"### 未来 {window} 天", ""])
            lines.append(_upcoming_table(report.upcoming_events(window)))
            lines.append("")
    lines.extend(_issue_section(report.issues))
    return "\n".join(lines).rstrip() + "\n"


def _upcoming_table(events: tuple[CatalystEvent, ...]) -> str:
    if not events:
        return "暂无已登记 upcoming catalyst。"
    lines = [
        (
            "| Date | Catalyst | Type | Importance | Ticker | Nodes | Pre-event | "
            "Post-event | Source |"
        ),
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for event in events:
        lines.append(
            "| "
            f"{event.event_date.isoformat()} | "
            f"`{event.catalyst_id}` {event.title} | "
            f"{event.event_type} | "
            f"{event.importance} | "
            f"{_escape_markdown_table(', '.join(event.related_tickers) or '无')} | "
            f"{_escape_markdown_table(', '.join(event.related_nodes) or '无')} | "
            f"{_escape_markdown_table(', '.join(event.pre_event_actions) or '无')} | "
            f"{_escape_markdown_table(', '.join(event.post_event_review_targets) or '无')} | "
            f"{_escape_markdown_table(event.source_name)} |"
        )
    return "\n".join(lines)


def _check_duplicate_catalyst_ids(
    calendar: CatalystCalendar,
    issues: list[CatalystCalendarIssue],
) -> None:
    counts = Counter(event.catalyst_id for event in calendar.events)
    for catalyst_id, count in counts.items():
        if count > 1:
            issues.append(
                CatalystCalendarIssue(
                    severity=CatalystCalendarIssueSeverity.ERROR,
                    code="duplicate_catalyst_id",
                    catalyst_id=catalyst_id,
                    message="catalyst_id 必须唯一。",
                )
            )


def _check_calendar_review_due(
    calendar: CatalystCalendar,
    as_of: date,
    issues: list[CatalystCalendarIssue],
) -> None:
    if calendar.next_review_due < as_of:
        issues.append(
            CatalystCalendarIssue(
                severity=CatalystCalendarIssueSeverity.WARNING,
                code="catalyst_calendar_review_overdue",
                message=(
                    "catalyst calendar 已超过 next_review_due："
                    f"{calendar.next_review_due.isoformat()}"
                ),
            )
        )


def _check_event(
    event: CatalystEvent,
    *,
    as_of: date,
    known_nodes: set[str],
    known_tickers: set[str],
    known_risk_events: set[str],
    issues: list[CatalystCalendarIssue],
) -> None:
    if event.reviewed_at > as_of:
        issues.append(
            CatalystCalendarIssue(
                severity=CatalystCalendarIssueSeverity.ERROR,
                code="reviewed_at_after_as_of",
                catalyst_id=event.catalyst_id,
                message="reviewed_at 不得晚于本次 as_of。",
            )
        )
    if event.captured_at.date() > as_of:
        issues.append(
            CatalystCalendarIssue(
                severity=CatalystCalendarIssueSeverity.ERROR,
                code="captured_at_after_as_of",
                catalyst_id=event.catalyst_id,
                message="captured_at 不得晚于本次 as_of。",
            )
        )
    if event.status in {"scheduled", "tentative"} and event.event_date < as_of:
        issues.append(
            CatalystCalendarIssue(
                severity=CatalystCalendarIssueSeverity.WARNING,
                code="scheduled_event_in_past",
                catalyst_id=event.catalyst_id,
                message="scheduled/tentative catalyst 的 event_date 已在 as_of 之前。",
            )
        )
    for node_id in event.related_nodes:
        if node_id not in known_nodes:
            issues.append(
                CatalystCalendarIssue(
                    severity=CatalystCalendarIssueSeverity.ERROR,
                    code="unknown_related_node",
                    catalyst_id=event.catalyst_id,
                    message=f"related_nodes 引用了未知产业链节点：{node_id}",
                )
            )
    for ticker in event.related_tickers:
        if ticker not in known_tickers:
            issues.append(
                CatalystCalendarIssue(
                    severity=CatalystCalendarIssueSeverity.ERROR,
                    code="unknown_related_ticker",
                    catalyst_id=event.catalyst_id,
                    message=f"related_tickers 引用了未知 ticker：{ticker}",
                )
            )
    for risk_event_id in event.linked_risk_event_ids:
        if risk_event_id not in known_risk_events:
            issues.append(
                CatalystCalendarIssue(
                    severity=CatalystCalendarIssueSeverity.ERROR,
                    code="unknown_linked_risk_event",
                    catalyst_id=event.catalyst_id,
                    message=f"linked_risk_event_ids 引用了未知风险事件：{risk_event_id}",
                )
            )
    if IMPORTANCE_ORDER[event.importance] >= IMPORTANCE_ORDER["high"]:
        if "pre_event_review" not in event.pre_event_actions:
            issues.append(
                CatalystCalendarIssue(
                    severity=CatalystCalendarIssueSeverity.WARNING,
                    code="high_importance_without_pre_event_review",
                    catalyst_id=event.catalyst_id,
                    message="high/critical catalyst 应包含 pre_event_review。",
                )
            )
        if not event.post_event_review_targets:
            issues.append(
                CatalystCalendarIssue(
                    severity=CatalystCalendarIssueSeverity.WARNING,
                    code="high_importance_without_post_event_review",
                    catalyst_id=event.catalyst_id,
                    message="high/critical catalyst 应声明事件后复核目标。",
                )
            )
    if event.source_type == "public_convenience" and IMPORTANCE_ORDER[event.importance] >= 3:
        issues.append(
            CatalystCalendarIssue(
                severity=CatalystCalendarIssueSeverity.WARNING,
                code="high_importance_public_convenience_source",
                catalyst_id=event.catalyst_id,
                message="高重要性催化剂不应只依赖 public_convenience 来源。",
            )
        )


def _known_tickers(
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    risk_events: RiskEventsConfig,
) -> set[str]:
    tickers = {item.ticker for item in watchlist.items}
    for node in industry_chain.nodes:
        tickers.update(node.related_tickers)
    for event in risk_events.event_rules:
        tickers.update(event.related_tickers)
    return tickers


def _issue_section(issues: tuple[CatalystCalendarIssue, ...]) -> list[str]:
    lines = ["", "## 校验事项", ""]
    if not issues:
        lines.append("未发现错误或警告。")
        return lines
    lines.extend(["| Severity | Code | Catalyst | Message |", "|---|---|---|---|"])
    for issue in issues:
        lines.append(
            "| "
            f"{issue.severity.value} | "
            f"`{issue.code}` | "
            f"{issue.catalyst_id or ''} | "
            f"{_escape_markdown_table(issue.message)} |"
        )
    return lines


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _compact_validation_error(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors():
        location = ".".join(str(item) for item in error.get("loc", ())) or "<root>"
        parts.append(f"{location}: {error.get('msg', '')}")
    return "; ".join(parts)
