from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path

from ai_trading_system.config import (
    IndustryChainConfig,
    RiskEventRuleConfig,
    RiskEventsConfig,
    UniverseConfig,
    WatchlistConfig,
    configured_price_tickers,
)


class RiskEventIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class RiskEventIssue:
    severity: RiskEventIssueSeverity
    code: str
    message: str
    event_id: str | None = None
    level: str | None = None


@dataclass(frozen=True)
class RiskEventsValidationReport:
    as_of: date
    config: RiskEventsConfig
    issues: tuple[RiskEventIssue, ...] = field(default_factory=tuple)

    @property
    def active_rule_count(self) -> int:
        return sum(1 for rule in self.config.event_rules if rule.active)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == RiskEventIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == RiskEventIssueSeverity.WARNING)

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


def validate_risk_events_config(
    risk_events: RiskEventsConfig,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    universe: UniverseConfig,
    as_of: date,
) -> RiskEventsValidationReport:
    issues: list[RiskEventIssue] = []
    node_ids = {node.node_id for node in industry_chain.nodes}
    known_tickers = set(configured_price_tickers(universe, include_full_ai_chain=True))
    known_tickers.update(item.ticker for item in watchlist.items)

    _check_level_actions(risk_events, issues)
    for rule in risk_events.event_rules:
        _check_rule_references(rule, node_ids, known_tickers, issues)
        _check_rule_action_design(rule, issues)

    return RiskEventsValidationReport(
        as_of=as_of,
        config=risk_events,
        issues=tuple(issues),
    )


def render_risk_events_validation_report(report: RiskEventsValidationReport) -> str:
    lines = [
        "# 风险事件分级校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 风险等级数：{len(report.config.levels)}",
        f"- 风险事件规则数：{len(report.config.event_rules)}",
        f"- 活跃规则数：{report.active_rule_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 风险等级",
        "",
        "| 等级 | 名称 | AI 仓位乘数 | 人工复核 | 默认动作 |",
        "|---|---|---:|---|---|",
    ]

    for level in sorted(report.config.levels, key=lambda item: item.level):
        lines.append(
            "| "
            f"{level.level} | "
            f"{_escape_markdown_table(level.name)} | "
            f"{level.target_ai_exposure_multiplier:.0%} | "
            f"{'需要' if level.requires_manual_review else '不需要'} | "
            f"{_escape_markdown_table(level.default_action)} |"
        )

    lines.extend(
        [
            "",
            "## 事件规则",
            "",
            "| 事件 | 名称 | 等级 | 活跃 | 影响节点 | 相关标的 |",
            "|---|---|---|---|---|---|",
        ]
    )
    for rule in sorted(report.config.event_rules, key=lambda item: item.event_id):
        lines.append(
            "| "
            f"{rule.event_id} | "
            f"{_escape_markdown_table(rule.name)} | "
            f"{rule.level} | "
            f"{'是' if rule.active else '否'} | "
            f"{', '.join(rule.affected_nodes)} | "
            f"{', '.join(rule.related_tickers)} |"
        )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | 等级 | 事件 | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.level or ''} | "
                f"{issue.event_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 风险事件规则不直接触发交易，只改变风险评估、仓位折扣或人工复核状态。",
            "- L2/L3 事件必须进入人工复核；仓位动作仍需受组合上限和 thesis 约束。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_risk_events_validation_report(
    report: RiskEventsValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_risk_events_validation_report(report), encoding="utf-8")
    return output_path


def default_risk_events_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"risk_events_validation_{as_of.isoformat()}.md"


def _check_level_actions(
    risk_events: RiskEventsConfig,
    issues: list[RiskEventIssue],
) -> None:
    levels = {level.level: level for level in risk_events.levels}
    if (
        levels["L1"].target_ai_exposure_multiplier
        < levels["L2"].target_ai_exposure_multiplier
        or levels["L2"].target_ai_exposure_multiplier
        < levels["L3"].target_ai_exposure_multiplier
    ):
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.ERROR,
                code="non_monotonic_exposure_multiplier",
                message="风险等级越高，AI 仓位乘数不能更高。",
            )
        )

    for level_id in ("L2", "L3"):
        if not levels[level_id].requires_manual_review:
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="high_level_without_manual_review",
                    level=level_id,
                    message="L2/L3 风险事件必须要求人工复核。",
                )
            )


def _check_rule_references(
    rule: RiskEventRuleConfig,
    node_ids: set[str],
    known_tickers: set[str],
    issues: list[RiskEventIssue],
) -> None:
    for node_id in rule.affected_nodes:
        if node_id not in node_ids:
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="unknown_affected_node",
                    event_id=rule.event_id,
                    message=f"风险事件引用了不存在的产业链节点：{node_id}",
                )
            )

    for ticker in rule.related_tickers:
        if ticker not in known_tickers:
            issues.append(
                RiskEventIssue(
                    severity=RiskEventIssueSeverity.ERROR,
                    code="unknown_related_ticker",
                    event_id=rule.event_id,
                    message=f"风险事件引用了未配置的数据或观察池标的：{ticker}",
                )
            )


def _check_rule_action_design(
    rule: RiskEventRuleConfig,
    issues: list[RiskEventIssue],
) -> None:
    if not rule.active:
        return

    if rule.level in {"L2", "L3"} and not rule.escalation_conditions:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="missing_escalation_conditions",
                event_id=rule.event_id,
                message="活跃 L2/L3 风险事件建议配置升级条件，避免临时主观加码。",
            )
        )

    if rule.level in {"L2", "L3"} and not rule.deescalation_conditions:
        issues.append(
            RiskEventIssue(
                severity=RiskEventIssueSeverity.WARNING,
                code="missing_deescalation_conditions",
                event_id=rule.event_id,
                message="活跃 L2/L3 风险事件建议配置解除条件，避免风险消失后无法复位。",
            )
        )


def _severity_label(severity: RiskEventIssueSeverity) -> str:
    if severity == RiskEventIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
