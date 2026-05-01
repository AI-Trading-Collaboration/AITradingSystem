from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path

from ai_trading_system.config import (
    IndustryChainConfig,
    IndustryChainNodeConfig,
    WatchlistConfig,
)


class IndustryChainIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class IndustryChainIssue:
    severity: IndustryChainIssueSeverity
    code: str
    message: str
    node_id: str | None = None


@dataclass(frozen=True)
class IndustryChainValidationReport:
    as_of: date
    nodes: tuple[IndustryChainNodeConfig, ...]
    issues: tuple[IndustryChainIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == IndustryChainIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == IndustryChainIssueSeverity.WARNING
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


def validate_industry_chain_config(
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    as_of: date,
) -> IndustryChainValidationReport:
    issues: list[IndustryChainIssue] = []
    nodes = tuple(industry_chain.nodes)
    nodes_by_id = {node.node_id: node for node in nodes}

    _check_duplicate_node_ids(nodes, issues)
    _check_node_contents(nodes, issues)
    _check_parent_references(nodes, nodes_by_id, issues)
    _check_cycles(nodes, nodes_by_id, issues)
    _check_watchlist_node_coverage(watchlist, nodes_by_id, issues)

    return IndustryChainValidationReport(
        as_of=as_of,
        nodes=nodes,
        issues=tuple(issues),
    )


def render_industry_chain_validation_report(report: IndustryChainValidationReport) -> str:
    lines = [
        "# 产业链因果图校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 节点数：{len(report.nodes)}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 节点概览",
        "",
        "| 节点 | 名称 | 父节点 | 相关标的 | 影响周期 | 现金流相关性 | 情绪相关性 |",
        "|---|---|---|---|---|---|---|",
    ]

    for node in sorted(report.nodes, key=lambda item: item.node_id):
        lines.append(
            "| "
            f"{node.node_id} | "
            f"{_escape_markdown_table(node.name)} | "
            f"{', '.join(node.parent_node_ids) or '无'} | "
            f"{', '.join(node.related_tickers)} | "
            f"{_horizon_label(node.impact_horizon)} | "
            f"{_relevance_label(node.cash_flow_relevance)} | "
            f"{_relevance_label(node.sentiment_relevance)} |"
        )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | 节点 | 说明 |",
                "|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.node_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    return "\n".join(lines) + "\n"


def write_industry_chain_validation_report(
    report: IndustryChainValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_industry_chain_validation_report(report), encoding="utf-8")
    return output_path


def default_industry_chain_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"industry_chain_validation_{as_of.isoformat()}.md"


def _check_duplicate_node_ids(
    nodes: tuple[IndustryChainNodeConfig, ...],
    issues: list[IndustryChainIssue],
) -> None:
    counts: dict[str, int] = {}
    for node in nodes:
        counts[node.node_id] = counts.get(node.node_id, 0) + 1

    for node_id, count in sorted(counts.items()):
        if count > 1:
            issues.append(
                IndustryChainIssue(
                    severity=IndustryChainIssueSeverity.ERROR,
                    code="duplicate_node_id",
                    node_id=node_id,
                    message="产业链节点 ID 重复，因果图无法可靠引用。",
                )
            )


def _check_node_contents(
    nodes: tuple[IndustryChainNodeConfig, ...],
    issues: list[IndustryChainIssue],
) -> None:
    for node in nodes:
        if not node.leading_indicators:
            issues.append(
                IndustryChainIssue(
                    severity=IndustryChainIssueSeverity.ERROR,
                    code="missing_leading_indicators",
                    node_id=node.node_id,
                    message="产业链节点必须至少配置一个领先指标。",
                )
            )
        if not node.related_tickers:
            issues.append(
                IndustryChainIssue(
                    severity=IndustryChainIssueSeverity.ERROR,
                    code="missing_related_tickers",
                    node_id=node.node_id,
                    message="产业链节点必须至少配置一个相关标的。",
                )
            )
        if node.node_id in node.parent_node_ids:
            issues.append(
                IndustryChainIssue(
                    severity=IndustryChainIssueSeverity.ERROR,
                    code="self_parent",
                    node_id=node.node_id,
                    message="产业链节点不能把自身配置为父节点。",
                )
            )


def _check_parent_references(
    nodes: tuple[IndustryChainNodeConfig, ...],
    nodes_by_id: dict[str, IndustryChainNodeConfig],
    issues: list[IndustryChainIssue],
) -> None:
    for node in nodes:
        for parent_node_id in node.parent_node_ids:
            if parent_node_id not in nodes_by_id:
                issues.append(
                    IndustryChainIssue(
                        severity=IndustryChainIssueSeverity.ERROR,
                        code="missing_parent_node",
                        node_id=node.node_id,
                        message=f"父节点不存在：{parent_node_id}",
                    )
                )


def _check_cycles(
    nodes: tuple[IndustryChainNodeConfig, ...],
    nodes_by_id: dict[str, IndustryChainNodeConfig],
    issues: list[IndustryChainIssue],
) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str, path: tuple[str, ...]) -> None:
        if node_id in visited:
            return
        if node_id in visiting:
            cycle_path = " -> ".join((*path, node_id))
            issues.append(
                IndustryChainIssue(
                    severity=IndustryChainIssueSeverity.ERROR,
                    code="cycle_detected",
                    node_id=node_id,
                    message=f"产业链因果图存在环：{cycle_path}",
                )
            )
            return

        node = nodes_by_id.get(node_id)
        if node is None:
            return

        visiting.add(node_id)
        for parent_node_id in node.parent_node_ids:
            visit(parent_node_id, (*path, node_id))
        visiting.remove(node_id)
        visited.add(node_id)

    for node in nodes:
        visit(node.node_id, ())


def _check_watchlist_node_coverage(
    watchlist: WatchlistConfig,
    nodes_by_id: dict[str, IndustryChainNodeConfig],
    issues: list[IndustryChainIssue],
) -> None:
    for item in watchlist.items:
        if not item.active:
            continue
        for node_id in item.ai_chain_nodes:
            if node_id not in nodes_by_id:
                issues.append(
                    IndustryChainIssue(
                        severity=IndustryChainIssueSeverity.ERROR,
                        code="watchlist_node_missing",
                        node_id=node_id,
                        message=f"{item.ticker} 引用了不存在的产业链节点。",
                    )
                )


def _horizon_label(value: str) -> str:
    return {
        "short": "短期",
        "medium": "中期",
        "long": "长期",
    }.get(value, value)


def _relevance_label(value: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
    }.get(value, value)


def _severity_label(severity: IndustryChainIssueSeverity) -> str:
    if severity == IndustryChainIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
