from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path

from ai_trading_system.config import UniverseConfig, WatchlistConfig, WatchlistItem


class WatchlistIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class WatchlistIssue:
    severity: WatchlistIssueSeverity
    code: str
    message: str
    ticker: str | None = None


@dataclass(frozen=True)
class WatchlistValidationReport:
    as_of: date
    items: tuple[WatchlistItem, ...]
    core_watchlist: tuple[str, ...]
    issues: tuple[WatchlistIssue, ...] = field(default_factory=tuple)

    @property
    def active_count(self) -> int:
        return sum(1 for item in self.items if item.active)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == WatchlistIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == WatchlistIssueSeverity.WARNING)

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


def validate_watchlist_config(
    watchlist: WatchlistConfig,
    universe: UniverseConfig,
    as_of: date,
    low_competence_threshold: float = 60.0,
) -> WatchlistValidationReport:
    issues: list[WatchlistIssue] = []
    items = tuple(watchlist.items)
    active_items = [item for item in items if item.active]
    active_by_ticker = {item.ticker: item for item in active_items}
    core_watchlist = tuple(universe.ai_chain.get("core_watchlist", []))

    _check_duplicate_tickers(items, issues)
    _check_core_watchlist_coverage(core_watchlist, active_by_ticker, issues)
    _check_active_item_rules(active_items, issues, low_competence_threshold)

    return WatchlistValidationReport(
        as_of=as_of,
        items=items,
        core_watchlist=core_watchlist,
        issues=tuple(issues),
    )


def render_watchlist_validation_report(report: WatchlistValidationReport) -> str:
    lines = [
        "# 观察池校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 配置标的数：{len(report.items)}",
        f"- 活跃标的数：{report.active_count}",
        f"- 核心观察池：{', '.join(report.core_watchlist)}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 活跃观察池",
        "",
        "| Ticker | 公司 | 类型 | 能力圈分数 | 默认风险 | Thesis | 产业链节点 |",
        "|---|---|---|---:|---|---|---|",
    ]

    active_items = (item for item in report.items if item.active)
    for item in sorted(active_items, key=lambda item: item.ticker):
        lines.append(
            "| "
            f"{item.ticker} | "
            f"{_escape_markdown_table(item.company_name)} | "
            f"{item.instrument_type} | "
            f"{item.competence_score:.0f} | "
            f"{_risk_level_label(item.default_risk_level)} | "
            f"{'需要' if item.thesis_required else '不需要'} | "
            f"{', '.join(item.ai_chain_nodes)} |"
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

    return "\n".join(lines) + "\n"


def write_watchlist_validation_report(
    report: WatchlistValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_watchlist_validation_report(report), encoding="utf-8")
    return output_path


def default_watchlist_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"watchlist_validation_{as_of.isoformat()}.md"


def _check_duplicate_tickers(
    items: tuple[WatchlistItem, ...],
    issues: list[WatchlistIssue],
) -> None:
    counts: dict[str, int] = {}
    for item in items:
        counts[item.ticker] = counts.get(item.ticker, 0) + 1

    duplicates = sorted(ticker for ticker, count in counts.items() if count > 1)
    for ticker in duplicates:
        issues.append(
            WatchlistIssue(
                severity=WatchlistIssueSeverity.ERROR,
                code="duplicate_ticker",
                ticker=ticker,
                message="观察池中存在重复 ticker，后续评分和复盘无法可靠引用。",
            )
        )


def _check_core_watchlist_coverage(
    core_watchlist: tuple[str, ...],
    active_by_ticker: dict[str, WatchlistItem],
    issues: list[WatchlistIssue],
) -> None:
    for ticker in core_watchlist:
        item = active_by_ticker.get(ticker)
        if item is None:
            issues.append(
                WatchlistIssue(
                    severity=WatchlistIssueSeverity.ERROR,
                    code="core_ticker_missing",
                    ticker=ticker,
                    message="核心观察池标的未在活跃观察池配置中出现。",
                )
            )
            continue

        if not item.ai_chain_nodes:
            issues.append(
                WatchlistIssue(
                    severity=WatchlistIssueSeverity.ERROR,
                    code="core_ticker_missing_ai_chain_nodes",
                    ticker=ticker,
                    message="核心观察池标的必须映射到至少一个 AI 产业链节点。",
                )
            )


def _check_active_item_rules(
    active_items: list[WatchlistItem],
    issues: list[WatchlistIssue],
    low_competence_threshold: float,
) -> None:
    for item in active_items:
        if not item.ai_chain_nodes:
            issues.append(
                WatchlistIssue(
                    severity=WatchlistIssueSeverity.ERROR,
                    code="active_ticker_missing_ai_chain_nodes",
                    ticker=item.ticker,
                    message="活跃观察池标的必须映射到至少一个 AI 产业链节点。",
                )
            )

        if item.default_risk_level in {"high", "critical"} and not item.thesis_required:
            issues.append(
                WatchlistIssue(
                    severity=WatchlistIssueSeverity.ERROR,
                    code="high_risk_without_thesis",
                    ticker=item.ticker,
                    message="高风险或极高风险标的必须要求交易 thesis。",
                )
            )

        if item.competence_score < low_competence_threshold:
            issues.append(
                WatchlistIssue(
                    severity=WatchlistIssueSeverity.WARNING,
                    code="low_competence_score",
                    ticker=item.ticker,
                    message=(
                        "能力圈分数较低，后续报告不能把该标的默认视为高置信度输入。"
                    ),
                )
            )


def _risk_level_label(level: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
        "critical": "极高",
    }.get(level, level)


def _severity_label(severity: WatchlistIssueSeverity) -> str:
    if severity == WatchlistIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
