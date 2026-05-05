from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH = PROJECT_ROOT / "config" / "feature_availability.yaml"
ALLOWED_MISSING_AVAILABLE_TIME_POLICIES = {"downgrade_to_c", "exclude_from_a_b"}
ALLOWED_MINIMUM_BACKTEST_GRADES = {"A", "B", "C"}


@dataclass(frozen=True)
class FeatureAvailabilityRule:
    rule_id: str
    family: str
    label: str
    sources: tuple[str, ...]
    event_time: str
    source_published_at: str
    available_time: str
    decision_time: str
    default_lag_days: int
    minimum_backtest_grade: str
    missing_available_time_policy: str
    enforced_in: tuple[str, ...]
    notes: str


@dataclass(frozen=True)
class FeatureAvailabilityIssue:
    severity: str
    code: str
    message: str
    rule_id: str | None = None
    source: str | None = None


@dataclass(frozen=True)
class FeatureAvailabilitySourceCheck:
    source: str
    input_path: Path | None
    row_count: int
    decision_time: date
    event_time_column: str
    available_time_column: str
    available_time_coverage_pct: float
    missing_available_time_count: int
    future_available_time_count: int
    fallback_policy: str = ""
    notes: str = ""

    @property
    def status(self) -> str:
        if self.future_available_time_count:
            return "FAIL"
        if self.row_count and not self.available_time_column and not self.fallback_policy:
            return "FAIL"
        if self.missing_available_time_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class FeatureAvailabilityReport:
    as_of: date
    input_path: Path
    generated_at: datetime
    rules: tuple[FeatureAvailabilityRule, ...]
    issues: tuple[FeatureAvailabilityIssue, ...]
    observed_sources: tuple[str, ...]
    required_sources: tuple[str, ...]
    source_checks: tuple[FeatureAvailabilitySourceCheck, ...] = ()
    production_effect: str = "none"

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

    @property
    def passed(self) -> bool:
        return self.error_count == 0


def default_feature_availability_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"feature_availability_{as_of.isoformat()}.md"


def build_feature_availability_report(
    *,
    input_path: Path = DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    as_of: date,
    observed_sources: tuple[str, ...] = (),
    required_sources: tuple[str, ...] = (),
    source_checks: tuple[FeatureAvailabilitySourceCheck, ...] = (),
) -> FeatureAvailabilityReport:
    issues: list[FeatureAvailabilityIssue] = []
    rules: tuple[FeatureAvailabilityRule, ...] = ()
    if not input_path.exists():
        issues.append(
            FeatureAvailabilityIssue(
                severity="ERROR",
                code="feature_availability_config_missing",
                message=f"feature availability catalog 不存在：{input_path}",
            )
        )
    else:
        try:
            raw = yaml.safe_load(input_path.read_text(encoding="utf-8")) or {}
            rules = _parse_rules(raw.get("rules", ()), issues)
        except (OSError, yaml.YAMLError, TypeError, ValueError) as exc:
            issues.append(
                FeatureAvailabilityIssue(
                    severity="ERROR",
                    code="feature_availability_config_unreadable",
                    message=f"feature availability catalog 无法读取：{exc}",
                )
            )
    issues.extend(_coverage_issues(rules, observed_sources, required_sources))
    issues.extend(_source_check_issues(rules, source_checks))
    return FeatureAvailabilityReport(
        as_of=as_of,
        input_path=input_path,
        generated_at=datetime.now(tz=UTC),
        rules=rules,
        issues=tuple(issues),
        observed_sources=tuple(sorted(set(observed_sources))),
        required_sources=tuple(sorted(set(required_sources))),
        source_checks=tuple(sorted(source_checks, key=lambda item: item.source)),
    )


def build_feature_source_check(
    *,
    source: str,
    frame: pd.DataFrame,
    decision_time: date,
    input_path: Path | None = None,
    event_time_columns: tuple[str, ...] = ("event_time", "date", "as_of", "end_date"),
    available_time_columns: tuple[str, ...] = (
        "available_time",
        "vendor_available_at",
        "captured_at",
        "ingested_at",
        "downloaded_at",
        "filed_date",
        "accepted_time",
        "reviewed_at",
    ),
    fallback_policy: str = "",
    notes: str = "",
) -> FeatureAvailabilitySourceCheck:
    """Inspect one concrete input frame for signal-time available_time discipline."""

    row_count = len(frame)
    event_time_column = _first_present_column(frame, event_time_columns)
    available_time_column = _first_present_column(frame, available_time_columns)
    missing_count = 0
    future_count = 0
    coverage = 0.0 if row_count else 1.0
    if available_time_column:
        available = pd.to_datetime(
            frame[available_time_column],
            errors="coerce",
            utc=True,
        )
        missing_count = int(available.isna().sum())
        coverage = 1.0 - (missing_count / row_count) if row_count else 1.0
        available_dates = available.dt.date
        future_count = int((available_dates > decision_time).sum())
    return FeatureAvailabilitySourceCheck(
        source=source,
        input_path=input_path,
        row_count=row_count,
        decision_time=decision_time,
        event_time_column=event_time_column,
        available_time_column=available_time_column,
        available_time_coverage_pct=coverage,
        missing_available_time_count=missing_count,
        future_available_time_count=future_count,
        fallback_policy=fallback_policy,
        notes=notes,
    )


def write_feature_availability_report(
    report: FeatureAvailabilityReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_feature_availability_report(report), encoding="utf-8")
    return output_path


def render_feature_availability_report(report: FeatureAvailabilityReport) -> str:
    lines = [
        "# PIT 特征可见时间报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- Catalog：`{report.input_path}`",
        f"- 规则数：{len(report.rules)}",
        f"- 观察到的 source：{_join(report.observed_sources)}",
        f"- 必需 source：{_join(report.required_sources)}",
        f"- 字段级 source 检查：{len(report.source_checks)}",
        f"- production_effect={report.production_effect}",
        "",
        "## 规则目录",
        "",
        (
            "| Rule | 输入族 | Source | Event Time | Published At | Available Time | "
            "Decision Time | 默认滞后 | A/B 缺口策略 | 最低等级 |"
        ),
        "|---|---|---|---|---|---|---|---:|---|---|",
    ]
    for rule in report.rules:
        lines.append(_rule_row(rule))
    if not report.rules:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a | 0 | n/a | n/a |")

    lines.extend(["", "## 覆盖检查", ""])
    if not report.issues:
        lines.append("未发现 feature availability 问题。")
    else:
        lines.extend(
            [
                "| Severity | Code | Rule | Source | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity} | "
                f"{issue.code} | "
                f"{issue.rule_id or ''} | "
                f"{issue.source or ''} | "
                f"{_escape(issue.message)} |"
            )

    lines.extend(["", "## 字段级 Source 检查", ""])
    if not report.source_checks:
        lines.append("未提供字段级 source 检查。")
    else:
        lines.extend(
            [
                (
                    "| Source | 状态 | 行数 | Event Time 字段 | Available Time 字段 | "
                    "覆盖率 | 未来可见时间 | Fallback | 输入 |"
                ),
                "|---|---|---:|---|---|---:|---:|---|---|",
            ]
        )
        for check in report.source_checks:
            lines.append(_source_check_row(check))

    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            (
                "- 本报告定义 `event_time` 与 `available_time` 的区别；"
                "历史回测不得只用 event date 判断可见性。"
            ),
            "- 缺少 `available_time` 的新特征默认只能进入 C 级探索，或被排除在 A/B 级主结论之外。",
            "- 日线信号按收盘后生成、下一交易日收益生效；报告中的默认滞后是保守审计假设。",
        ]
    )
    return "\n".join(lines) + "\n"


def render_feature_availability_section(
    report: FeatureAvailabilityReport,
    report_path: Path | None = None,
) -> str:
    lines = [
        "## PIT 特征可见时间",
        "",
        f"- 状态：{report.status}",
        f"- 规则数：{len(report.rules)}",
        f"- 字段级 source 检查：{len(report.source_checks)}",
        f"- 观察 source：{_join(report.observed_sources)}",
        f"- 错误/警告：{report.error_count}/{report.warning_count}",
        "- A/B 级约束：缺少 `available_time` 的特征不得进入主结论。",
    ]
    if report_path is not None:
        lines.append(f"- 独立报告：`{report_path}`")
    family_counts = Counter(rule.family for rule in report.rules)
    if family_counts:
        lines.extend(
            [
                "",
                "| 输入族 | 规则数 |",
                "|---|---:|",
                *[
                    f"| {family} | {family_counts[family]} |"
                    for family in sorted(family_counts)
                ],
            ]
        )
    return "\n".join(lines)


def feature_availability_summary_record(
    report: FeatureAvailabilityReport,
    report_path: Path | None = None,
) -> dict[str, object]:
    return {
        "status": report.status,
        "report_path": None if report_path is None else str(report_path),
        "catalog_path": str(report.input_path),
        "rule_count": len(report.rules),
        "observed_sources": list(report.observed_sources),
        "required_sources": list(report.required_sources),
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "source_checks": [
            {
                "source": check.source,
                "status": check.status,
                "input_path": None if check.input_path is None else str(check.input_path),
                "row_count": check.row_count,
                "decision_time": check.decision_time.isoformat(),
                "event_time_column": check.event_time_column,
                "available_time_column": check.available_time_column,
                "available_time_coverage_pct": check.available_time_coverage_pct,
                "missing_available_time_count": check.missing_available_time_count,
                "future_available_time_count": check.future_available_time_count,
                "fallback_policy": check.fallback_policy,
            }
            for check in report.source_checks
        ],
        "rules": [
            {
                "rule_id": rule.rule_id,
                "family": rule.family,
                "sources": list(rule.sources),
                "available_time": rule.available_time,
                "decision_time": rule.decision_time,
                "default_lag_days": rule.default_lag_days,
                "minimum_backtest_grade": rule.minimum_backtest_grade,
                "missing_available_time_policy": rule.missing_available_time_policy,
            }
            for rule in report.rules
        ],
    }


def _parse_rules(
    raw_rules: object,
    issues: list[FeatureAvailabilityIssue],
) -> tuple[FeatureAvailabilityRule, ...]:
    if not isinstance(raw_rules, list):
        issues.append(
            FeatureAvailabilityIssue(
                severity="ERROR",
                code="feature_availability_rules_not_list",
                message="rules 必须是列表。",
            )
        )
        return ()
    rules: list[FeatureAvailabilityRule] = []
    seen: set[str] = set()
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, dict):
            issues.append(
                FeatureAvailabilityIssue(
                    severity="ERROR",
                    code="feature_availability_rule_not_object",
                    message="每条 feature availability rule 必须是对象。",
                )
            )
            continue
        rule = _rule_from_mapping(raw_rule, issues)
        if rule is None:
            continue
        if rule.rule_id in seen:
            issues.append(
                FeatureAvailabilityIssue(
                    severity="ERROR",
                    code="duplicate_feature_availability_rule",
                    message=f"重复 rule_id：{rule.rule_id}",
                    rule_id=rule.rule_id,
                )
            )
        seen.add(rule.rule_id)
        rules.append(rule)
    return tuple(rules)


def _rule_from_mapping(
    raw: dict[str, Any],
    issues: list[FeatureAvailabilityIssue],
) -> FeatureAvailabilityRule | None:
    rule_id = str(raw.get("rule_id") or "").strip()
    required_text_fields = (
        "family",
        "label",
        "event_time",
        "source_published_at",
        "available_time",
        "decision_time",
        "missing_available_time_policy",
        "minimum_backtest_grade",
    )
    missing = [field for field in required_text_fields if not str(raw.get(field) or "").strip()]
    sources = tuple(str(item).strip() for item in raw.get("sources") or () if str(item).strip())
    if not rule_id:
        missing.append("rule_id")
    if not sources:
        missing.append("sources")
    if missing:
        issues.append(
            FeatureAvailabilityIssue(
                severity="ERROR",
                code="feature_availability_rule_missing_fields",
                message=f"规则缺少字段：{', '.join(sorted(set(missing)))}",
                rule_id=rule_id or None,
            )
        )
        return None
    policy = str(raw["missing_available_time_policy"]).strip()
    if policy not in ALLOWED_MISSING_AVAILABLE_TIME_POLICIES:
        issues.append(
            FeatureAvailabilityIssue(
                severity="ERROR",
                code="invalid_missing_available_time_policy",
                message=f"未知 missing_available_time_policy：{policy}",
                rule_id=rule_id,
            )
        )
    grade = str(raw["minimum_backtest_grade"]).strip().upper()
    if grade not in ALLOWED_MINIMUM_BACKTEST_GRADES:
        issues.append(
            FeatureAvailabilityIssue(
                severity="ERROR",
                code="invalid_minimum_backtest_grade",
                message=f"未知 minimum_backtest_grade：{grade}",
                rule_id=rule_id,
            )
        )
    lag = int(raw.get("default_lag_days", 0))
    if lag < 0:
        issues.append(
            FeatureAvailabilityIssue(
                severity="ERROR",
                code="negative_default_lag_days",
                message="default_lag_days 不能为负数。",
                rule_id=rule_id,
            )
        )
    return FeatureAvailabilityRule(
        rule_id=rule_id,
        family=str(raw["family"]).strip(),
        label=str(raw["label"]).strip(),
        sources=sources,
        event_time=str(raw["event_time"]).strip(),
        source_published_at=str(raw["source_published_at"]).strip(),
        available_time=str(raw["available_time"]).strip(),
        decision_time=str(raw["decision_time"]).strip(),
        default_lag_days=lag,
        minimum_backtest_grade=grade,
        missing_available_time_policy=policy,
        enforced_in=tuple(str(item).strip() for item in raw.get("enforced_in") or ()),
        notes=str(raw.get("notes") or "").strip(),
    )


def _coverage_issues(
    rules: tuple[FeatureAvailabilityRule, ...],
    observed_sources: tuple[str, ...],
    required_sources: tuple[str, ...],
) -> tuple[FeatureAvailabilityIssue, ...]:
    source_to_rule = {
        source
        for rule in rules
        for source in rule.sources
    }
    issues: list[FeatureAvailabilityIssue] = []
    for source in sorted(set(observed_sources) - source_to_rule):
        issues.append(
            FeatureAvailabilityIssue(
                severity="ERROR",
                code="observed_source_without_availability_rule",
                message="观察到的特征 source 没有可见时间规则。",
                source=source,
            )
        )
    for source in sorted(set(required_sources) - source_to_rule):
        issues.append(
            FeatureAvailabilityIssue(
                severity="ERROR",
                code="required_source_without_availability_rule",
                message="必需 source 没有可见时间规则。",
                source=source,
            )
        )
    return tuple(issues)


def _source_check_issues(
    rules: tuple[FeatureAvailabilityRule, ...],
    checks: tuple[FeatureAvailabilitySourceCheck, ...],
) -> tuple[FeatureAvailabilityIssue, ...]:
    rule_by_source = {
        source: rule
        for rule in rules
        for source in rule.sources
    }
    issues: list[FeatureAvailabilityIssue] = []
    for check in checks:
        rule = rule_by_source.get(check.source)
        if rule is None:
            continue
        if check.future_available_time_count:
            issues.append(
                FeatureAvailabilityIssue(
                    severity="ERROR",
                    code="feature_source_available_time_after_decision_time",
                    rule_id=rule.rule_id,
                    source=check.source,
                    message=(
                        f"{check.future_available_time_count} 行 available_time 晚于 "
                        f"decision_time={check.decision_time.isoformat()}。"
                    ),
                )
            )
        if check.row_count and not check.available_time_column:
            if not check.fallback_policy:
                issues.append(
                    FeatureAvailabilityIssue(
                        severity="ERROR",
                        code="feature_source_available_time_column_missing",
                        rule_id=rule.rule_id,
                        source=check.source,
                        message=(
                            "输入缺少显式 available_time 字段，不能进入 A/B 级主结论。"
                        ),
                    )
                )
        if check.missing_available_time_count:
            severity = (
                "ERROR"
                if rule.missing_available_time_policy == "exclude_from_a_b"
                else "WARNING"
            )
            issues.append(
                FeatureAvailabilityIssue(
                    severity=severity,
                    code="feature_source_available_time_values_missing",
                    rule_id=rule.rule_id,
                    source=check.source,
                    message=(
                        f"{check.missing_available_time_count} 行 available_time 为空；"
                        f"策略={rule.missing_available_time_policy}。"
                    ),
                )
            )
    return tuple(issues)


def _rule_row(rule: FeatureAvailabilityRule) -> str:
    return (
        "| "
        f"{rule.rule_id} | "
        f"{rule.family} | "
        f"{_escape(', '.join(rule.sources))} | "
        f"{_escape(rule.event_time)} | "
        f"{_escape(rule.source_published_at)} | "
        f"{_escape(rule.available_time)} | "
        f"{_escape(rule.decision_time)} | "
        f"{rule.default_lag_days} | "
        f"{rule.missing_available_time_policy} | "
        f"{rule.minimum_backtest_grade} |"
    )


def _source_check_row(check: FeatureAvailabilitySourceCheck) -> str:
    return (
        "| "
        f"{check.source} | "
        f"{check.status} | "
        f"{check.row_count} | "
        f"{check.event_time_column or 'missing'} | "
        f"{check.available_time_column or 'missing'} | "
        f"{check.available_time_coverage_pct:.0%} | "
        f"{check.future_available_time_count} | "
        f"{_escape(check.fallback_policy) if check.fallback_policy else '无'} | "
        f"{_escape(str(check.input_path or ''))} |"
    )


def _first_present_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str:
    for column in candidates:
        if column in frame.columns:
            return column
    return ""


def _join(values: tuple[str, ...]) -> str:
    return ", ".join(values) if values else "无"


def _escape(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
