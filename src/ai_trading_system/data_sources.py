from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path

from ai_trading_system.config import DataSourceConfig, DataSourcesConfig


class DataSourceIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class DataSourceIssue:
    severity: DataSourceIssueSeverity
    code: str
    message: str
    source_id: str | None = None


@dataclass(frozen=True)
class DataSourcesValidationReport:
    as_of: date
    sources: tuple[DataSourceConfig, ...]
    issues: tuple[DataSourceIssue, ...] = field(default_factory=tuple)

    @property
    def active_count(self) -> int:
        return sum(1 for source in self.sources if source.status == "active")

    @property
    def planned_count(self) -> int:
        return sum(1 for source in self.sources if source.status == "planned")

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == DataSourceIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == DataSourceIssueSeverity.WARNING)

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


def validate_data_sources_config(
    config: DataSourcesConfig,
    as_of: date,
) -> DataSourcesValidationReport:
    sources = tuple(config.sources)
    issues: list[DataSourceIssue] = []

    _check_duplicate_source_ids(sources, issues)
    for source in sources:
        _check_active_source_auditability(source, issues)
        _check_source_type_constraints(source, issues)
        _check_planned_source_readiness(source, issues)

    return DataSourcesValidationReport(
        as_of=as_of,
        sources=sources,
        issues=tuple(issues),
    )


def render_data_sources_validation_report(report: DataSourcesValidationReport) -> str:
    lines = [
        "# 数据源目录校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 数据源数量：{len(report.sources)}",
        f"- 活跃数据源：{report.active_count}",
        f"- 计划数据源：{report.planned_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 数据源目录",
        "",
        "| Source | Provider | 类型 | 状态 | 领域 | Cadence | 缓存 |",
        "|---|---|---|---|---|---|---|",
    ]

    for source in sorted(report.sources, key=lambda item: item.source_id):
        lines.append(
            "| "
            f"{source.source_id} | "
            f"{_escape_markdown_table(source.provider)} | "
            f"{_source_type_label(source.source_type)} | "
            f"{_status_label(source.status)} | "
            f"{', '.join(source.domains)} | "
            f"{source.cadence} | "
            f"{_escape_markdown_table(', '.join(source.cache_paths))} |"
        )

    lines.extend(["", "## 审计要求", ""])
    active_sources = [source for source in report.sources if source.status == "active"]
    if not active_sources:
        lines.append("未配置活跃数据源。")
    else:
        lines.extend(
            [
                "| Source | Audit Fields | Validation Checks | 限制说明 |",
                "|---|---|---|---|",
            ]
        )
        for source in sorted(active_sources, key=lambda item: item.source_id):
            lines.append(
                "| "
                f"{source.source_id} | "
                f"{_escape_markdown_table(', '.join(source.audit_fields))} | "
                f"{_escape_markdown_table(', '.join(source.validation_checks))} | "
                f"{_escape_markdown_table('; '.join(source.limitations))} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Source | 说明 |",
                "|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.source_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 数据源目录只声明来源纪律，不代表该来源已经可进入自动评分。",
            "- 活跃来源必须有审计字段和校验项；计划来源进入实现前必须补齐验证路径。",
            "- `public_convenience` 和 `manual_input` 只能作为受限输入，报告必须说明限制。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_data_sources_validation_report(
    report: DataSourcesValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_data_sources_validation_report(report), encoding="utf-8")
    return output_path


def default_data_sources_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"data_sources_validation_{as_of.isoformat()}.md"


def _check_duplicate_source_ids(
    sources: tuple[DataSourceConfig, ...],
    issues: list[DataSourceIssue],
) -> None:
    counts: dict[str, int] = {}
    for source in sources:
        counts[source.source_id] = counts.get(source.source_id, 0) + 1

    for source_id, count in sorted(counts.items()):
        if count <= 1:
            continue
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="duplicate_source_id",
                source_id=source_id,
                message="数据源 source_id 重复，后续审计和报告引用无法可靠追踪。",
            )
        )


def _check_active_source_auditability(
    source: DataSourceConfig,
    issues: list[DataSourceIssue],
) -> None:
    if source.status != "active":
        return

    if not source.endpoint:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="active_source_missing_endpoint",
                source_id=source.source_id,
                message="活跃数据源必须声明 endpoint 或本地输入路径。",
            )
        )
    if not source.audit_fields:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="active_source_missing_audit_fields",
                source_id=source.source_id,
                message="活跃数据源必须声明审计字段。",
            )
        )
    if not source.validation_checks:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="active_source_missing_validation_checks",
                source_id=source.source_id,
                message="活跃数据源必须声明下游使用前的校验项。",
            )
        )

    required_fields = _required_audit_fields(source)
    missing_fields = sorted(required_fields - set(source.audit_fields))
    if missing_fields:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="active_source_missing_required_audit_fields",
                source_id=source.source_id,
                message=f"活跃数据源缺少必要审计字段：{', '.join(missing_fields)}",
            )
        )


def _check_source_type_constraints(
    source: DataSourceConfig,
    issues: list[DataSourceIssue],
) -> None:
    if source.source_type in {"public_convenience", "manual_input"} and not source.limitations:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="limited_source_without_limitations",
                source_id=source.source_id,
                message="公开便利源或手工输入必须明确限制说明。",
            )
        )

    sensitive_domains = {"fundamentals", "valuation", "news_events", "risk_events"}
    if (
        source.status == "active"
        and source.source_type == "public_convenience"
        and sensitive_domains.intersection(source.domains)
    ):
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="public_convenience_sensitive_domain",
                source_id=source.source_id,
                message="公开便利源用于基本面、估值或事件领域时，只能作为辅助输入。",
            )
        )


def _check_planned_source_readiness(
    source: DataSourceConfig,
    issues: list[DataSourceIssue],
) -> None:
    if source.status != "planned":
        return

    if source.cache_paths:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="planned_source_has_cache_path",
                source_id=source.source_id,
                message="计划数据源尚未实现，不应声明已落地缓存路径。",
            )
        )
    if not source.owner_notes:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="planned_source_missing_owner_notes",
                source_id=source.source_id,
                message="计划数据源需要记录接入前的决策说明或验收条件。",
            )
        )


def _required_audit_fields(source: DataSourceConfig) -> set[str]:
    if source.source_type == "manual_input":
        return {"provider", "file_path", "checksum"}
    return {"provider", "endpoint", "request_parameters", "downloaded_at", "row_count"}


def _source_type_label(source_type: str) -> str:
    return {
        "primary_source": "一手来源",
        "paid_vendor": "付费供应商",
        "public_convenience": "公开便利源",
        "manual_input": "手工输入",
    }.get(source_type, source_type)


def _status_label(status: str) -> str:
    return {
        "active": "已启用",
        "planned": "计划中",
        "inactive": "停用",
    }.get(status, status)


def _severity_label(severity: DataSourceIssueSeverity) -> str:
    if severity == DataSourceIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
