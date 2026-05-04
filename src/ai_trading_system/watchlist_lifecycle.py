from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import UniverseConfig, WatchlistConfig

DEFAULT_WATCHLIST_LIFECYCLE_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "watchlist_lifecycle.yaml"
)

CompetenceStatus = Literal[
    "in_competence",
    "watch_only",
    "out_of_competence",
    "unknown",
]


class WatchlistLifecycleIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class WatchlistLifecycleEntry(BaseModel):
    ticker: str = Field(min_length=1)
    added_at: date
    removed_at: date | None = None
    reason: str = Field(min_length=1)
    active_from: date
    active_until: date | None = None
    competence_status: CompetenceStatus = "unknown"
    node_mapping_valid_from: date
    thesis_required_from: date | None = None
    source: str = Field(min_length=1)
    reviewer: str = ""
    notes: str = ""

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        self.ticker = self.ticker.upper()
        if self.active_from < self.added_at:
            raise ValueError("active_from must be on or after added_at")
        if self.active_until is not None and self.active_until < self.active_from:
            raise ValueError("active_until must be on or after active_from")
        if self.removed_at is not None and self.removed_at < self.added_at:
            raise ValueError("removed_at must be on or after added_at")
        if self.thesis_required_from is not None and self.thesis_required_from < self.added_at:
            raise ValueError("thesis_required_from must be on or after added_at")
        return self


class WatchlistLifecycleConfig(BaseModel):
    entries: list[WatchlistLifecycleEntry] = Field(default_factory=list)


@dataclass(frozen=True)
class WatchlistLifecycleIssue:
    severity: WatchlistLifecycleIssueSeverity
    code: str
    message: str
    ticker: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class WatchlistLifecycleValidationReport:
    as_of: date
    input_path: Path
    lifecycle: WatchlistLifecycleConfig
    issues: tuple[WatchlistLifecycleIssue, ...] = field(default_factory=tuple)

    @property
    def entry_count(self) -> int:
        return len(self.lifecycle.entries)

    @property
    def active_entry_count(self) -> int:
        return sum(1 for entry in self.lifecycle.entries if _entry_active_as_of(entry, self.as_of))

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == WatchlistLifecycleIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == WatchlistLifecycleIssueSeverity.WARNING
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


def load_watchlist_lifecycle(
    path: Path | str = DEFAULT_WATCHLIST_LIFECYCLE_PATH,
) -> WatchlistLifecycleConfig:
    input_path = Path(path)
    with input_path.open("r", encoding="utf-8") as file:
        raw: Any = yaml.safe_load(file) or {}
    return WatchlistLifecycleConfig.model_validate(raw)


def validate_watchlist_lifecycle(
    *,
    lifecycle: WatchlistLifecycleConfig,
    input_path: Path,
    watchlist: WatchlistConfig,
    universe: UniverseConfig,
    as_of: date,
) -> WatchlistLifecycleValidationReport:
    issues: list[WatchlistLifecycleIssue] = []
    _check_duplicate_lifecycle_entries(lifecycle, input_path, issues)
    _check_watchlist_coverage(lifecycle, input_path, watchlist, universe, as_of, issues)
    return WatchlistLifecycleValidationReport(
        as_of=as_of,
        input_path=input_path,
        lifecycle=lifecycle,
        issues=tuple(issues),
    )


def active_watchlist_tickers_as_of(
    *,
    lifecycle: WatchlistLifecycleConfig,
    tickers: list[str] | tuple[str, ...],
    as_of: date,
    require_node_mapping: bool = True,
) -> list[str]:
    entries_by_ticker: dict[str, list[WatchlistLifecycleEntry]] = {}
    for entry in lifecycle.entries:
        entries_by_ticker.setdefault(entry.ticker, []).append(entry)

    active_tickers: list[str] = []
    for ticker in tickers:
        normalized = ticker.upper()
        if any(
            _entry_usable_as_of(
                entry,
                as_of=as_of,
                require_node_mapping=require_node_mapping,
            )
            for entry in entries_by_ticker.get(normalized, [])
        ):
            active_tickers.append(ticker)
    return active_tickers


def render_watchlist_lifecycle_report(report: WatchlistLifecycleValidationReport) -> str:
    lines = [
        "# 观察池生命周期校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.input_path}`",
        f"- 生命周期记录数：{report.entry_count}",
        f"- 当前活跃记录数：{report.active_entry_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 生命周期记录",
        "",
    ]
    if not report.lifecycle.entries:
        lines.append("未发现观察池生命周期记录。")
    else:
        lines.extend(
            [
                "| Ticker | Active From | Active Until | Added At | Removed At | "
                "Competence | Node Mapping From | Thesis From | Reason |",
                "|---|---|---|---|---|---|---|---|---|",
            ]
        )
        for entry in sorted(
            report.lifecycle.entries,
            key=lambda item: (item.ticker, item.active_from),
        ):
            lines.append(
                "| "
                f"{entry.ticker} | "
                f"{entry.active_from.isoformat()} | "
                f"{entry.active_until.isoformat() if entry.active_until else ''} | "
                f"{entry.added_at.isoformat()} | "
                f"{entry.removed_at.isoformat() if entry.removed_at else ''} | "
                f"{_competence_status_label(entry.competence_status)} | "
                f"{entry.node_mapping_valid_from.isoformat()} | "
                f"{entry.thesis_required_from.isoformat() if entry.thesis_required_from else ''} | "
                f"{_escape_markdown_table(entry.reason)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Ticker | 说明 |",
                "|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )
    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 回测按 signal_date 读取当时已进入观察池、能力圈有效且节点映射可见的 ticker。",
            "- lifecycle 不替代当前 `config/watchlist.yaml`，而是为历史回测提供 "
            "point-in-time 可见性边界。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_watchlist_lifecycle_report(
    report: WatchlistLifecycleValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_watchlist_lifecycle_report(report), encoding="utf-8")
    return output_path


def default_watchlist_lifecycle_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"watchlist_lifecycle_{as_of.isoformat()}.md"


def _check_duplicate_lifecycle_entries(
    lifecycle: WatchlistLifecycleConfig,
    input_path: Path,
    issues: list[WatchlistLifecycleIssue],
) -> None:
    seen: set[tuple[str, date]] = set()
    for entry in lifecycle.entries:
        key = (entry.ticker, entry.active_from)
        if key in seen:
            issues.append(
                WatchlistLifecycleIssue(
                    severity=WatchlistLifecycleIssueSeverity.ERROR,
                    code="duplicate_watchlist_lifecycle_entry",
                    ticker=entry.ticker,
                    path=input_path,
                    message="同一 ticker 和 active_from 的生命周期记录重复。",
                )
            )
        seen.add(key)


def _check_watchlist_coverage(
    lifecycle: WatchlistLifecycleConfig,
    input_path: Path,
    watchlist: WatchlistConfig,
    universe: UniverseConfig,
    as_of: date,
    issues: list[WatchlistLifecycleIssue],
) -> None:
    lifecycle_tickers = {entry.ticker for entry in lifecycle.entries}
    core_watchlist = set(universe.ai_chain.get("core_watchlist", []))
    active_watchlist = {item.ticker for item in watchlist.items if item.active}
    for ticker in sorted(core_watchlist | active_watchlist):
        if ticker not in lifecycle_tickers:
            issues.append(
                WatchlistLifecycleIssue(
                    severity=WatchlistLifecycleIssueSeverity.ERROR,
                    code="missing_watchlist_lifecycle",
                    ticker=ticker,
                    path=input_path,
                    message="当前核心观察池或活跃观察池 ticker 缺少 point-in-time 生命周期记录。",
                )
            )

    active_as_of = set(
        active_watchlist_tickers_as_of(
            lifecycle=lifecycle,
            tickers=tuple(core_watchlist | active_watchlist),
            as_of=as_of,
        )
    )
    for ticker in sorted(active_watchlist - active_as_of):
        issues.append(
            WatchlistLifecycleIssue(
                severity=WatchlistLifecycleIssueSeverity.WARNING,
                code="active_watchlist_not_active_in_lifecycle",
                ticker=ticker,
                path=input_path,
                message="当前 watchlist 标记活跃，但 lifecycle 在评估日未标记为可用于评分/回测。",
            )
        )


def _entry_usable_as_of(
    entry: WatchlistLifecycleEntry,
    *,
    as_of: date,
    require_node_mapping: bool,
) -> bool:
    if not _entry_active_as_of(entry, as_of):
        return False
    if entry.competence_status == "out_of_competence":
        return False
    if require_node_mapping and entry.node_mapping_valid_from > as_of:
        return False
    return True


def _entry_active_as_of(entry: WatchlistLifecycleEntry, as_of: date) -> bool:
    if entry.active_from > as_of:
        return False
    if entry.active_until is not None and entry.active_until < as_of:
        return False
    if entry.removed_at is not None and entry.removed_at <= as_of:
        return False
    return True


def _severity_label(severity: WatchlistLifecycleIssueSeverity) -> str:
    if severity == WatchlistLifecycleIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _competence_status_label(value: str) -> str:
    return {
        "in_competence": "能力圈内",
        "watch_only": "仅观察",
        "out_of_competence": "能力圈外",
        "unknown": "未知",
    }.get(value, value)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
