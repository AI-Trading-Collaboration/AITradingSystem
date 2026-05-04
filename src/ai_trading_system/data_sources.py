from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from glob import glob
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT, DataSourceConfig, DataSourcesConfig

DOWNLOAD_MANIFEST_REQUIRED_COLUMNS = (
    "downloaded_at",
    "source_id",
    "provider",
    "endpoint",
    "request_parameters",
    "output_path",
    "row_count",
    "checksum_sha256",
)
RECONCILIATION_REQUIRED_DOMAINS = (
    "market_prices",
    "macro_rates",
    "fundamentals",
    "valuation",
)


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
class DataSourceHealthIssue:
    severity: DataSourceIssueSeverity
    code: str
    message: str
    source_id: str | None = None
    domain: str | None = None


@dataclass(frozen=True)
class CachePathHealth:
    configured_path: str
    resolved_path: str
    exists: bool
    matched_path_count: int
    row_count: int | None = None
    checksum_sha256: str | None = None
    note: str = ""


@dataclass(frozen=True)
class ManifestHealthRecord:
    source_id: str
    provider: str
    endpoint: str
    request_parameters: str
    downloaded_at: datetime | None
    output_path: str
    row_count: int | None
    checksum_sha256: str | None


@dataclass(frozen=True)
class DataSourceHealthRecord:
    source: DataSourceConfig
    cache_paths: tuple[CachePathHealth, ...]
    latest_manifest: ManifestHealthRecord | None
    health_score: int


@dataclass(frozen=True)
class DomainReconciliationRecord:
    domain: str
    status: str
    active_source_ids: tuple[str, ...]
    qualified_source_ids: tuple[str, ...]


@dataclass(frozen=True)
class DataSourceHealthReport:
    as_of: date
    generated_at: datetime
    catalog_report: DataSourcesValidationReport
    manifest_path: Path
    manifest_exists: bool
    manifest_row_count: int
    records: tuple[DataSourceHealthRecord, ...]
    domain_reconciliation: tuple[DomainReconciliationRecord, ...]
    issues: tuple[DataSourceHealthIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        health_errors = sum(
            1 for issue in self.issues if issue.severity == DataSourceIssueSeverity.ERROR
        )
        return self.catalog_report.error_count + health_errors

    @property
    def warning_count(self) -> int:
        health_warnings = sum(
            1 for issue in self.issues if issue.severity == DataSourceIssueSeverity.WARNING
        )
        return self.catalog_report.warning_count + health_warnings

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

    @property
    def health_score(self) -> int:
        if not self.records:
            return 0
        return round(sum(record.health_score for record in self.records) / len(self.records))


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
        _check_llm_permission_constraints(source, issues)
        _check_planned_source_readiness(source, issues)

    return DataSourcesValidationReport(
        as_of=as_of,
        sources=sources,
        issues=tuple(issues),
    )


def build_data_source_health_report(
    config: DataSourcesConfig,
    as_of: date,
    manifest_path: Path,
    project_root: Path = PROJECT_ROOT,
) -> DataSourceHealthReport:
    catalog_report = validate_data_sources_config(config, as_of=as_of)
    issues: list[DataSourceHealthIssue] = []
    manifest = _read_download_manifest(manifest_path, issues)
    records: list[DataSourceHealthRecord] = []

    for source in config.sources:
        latest_manifest = _latest_manifest_record(source, manifest, issues, project_root)
        cache_paths = tuple(_cache_path_health(path, project_root) for path in source.cache_paths)
        _check_source_cache_health(source, cache_paths, issues)
        _check_source_manifest_health(source, latest_manifest, issues, as_of)
        source_issues = [issue for issue in issues if issue.source_id == source.source_id]
        errors = sum(
            1 for issue in source_issues if issue.severity == DataSourceIssueSeverity.ERROR
        )
        warnings = sum(
            1 for issue in source_issues if issue.severity == DataSourceIssueSeverity.WARNING
        )
        records.append(
            DataSourceHealthRecord(
                source=source,
                cache_paths=cache_paths,
                latest_manifest=latest_manifest,
                health_score=_provider_health_score(errors, warnings),
            )
        )

    domain_reconciliation = _build_domain_reconciliation(config, issues)
    return DataSourceHealthReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        catalog_report=catalog_report,
        manifest_path=manifest_path,
        manifest_exists=manifest is not None,
        manifest_row_count=0 if manifest is None else len(manifest),
        records=tuple(records),
        domain_reconciliation=domain_reconciliation,
        issues=tuple(issues),
    )


def render_data_source_health_report(report: DataSourceHealthReport) -> str:
    lines = [
        "# 数据源健康与 reconciliation 报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- Provider health score：{report.health_score}",
        f"- 数据源目录状态：{report.catalog_report.status}",
        f"- Download manifest：`{report.manifest_path}`",
        f"- Manifest 行数：{report.manifest_row_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 方法边界",
        "",
        "- 这是低成本基础版 provider health 和 reconciliation 框架，"
        "用于暴露来源覆盖、缓存、manifest、row count、checksum 和新鲜度问题。",
        "- 跨供应商冲突必须进入调查项；本报告不会自动平滑、覆盖或修正供应商数值。",
        "- 没有两个 qualified source 的领域会标记为 `NOT_COVERED`，"
        "不能视为生产级 cross-provider reconciliation 已完成。",
        "",
        "## Reconciliation 覆盖",
        "",
        "| 领域 | 状态 | 活跃来源 | Qualified sources |",
        "|---|---|---|---|",
    ]
    for item in report.domain_reconciliation:
        lines.append(
            "| "
            f"{item.domain} | "
            f"{item.status} | "
            f"{_escape_markdown_table(', '.join(item.active_source_ids) or '无')} | "
            f"{_escape_markdown_table(', '.join(item.qualified_source_ids) or '无')} |"
        )

    lines.extend(
        [
            "",
            "## Provider Health",
            "",
            "| Source | Provider | 类型 | 领域 | Health | Latest downloaded_at | "
            "Row count | Checksum | Cache |",
            "|---|---|---|---|---:|---|---:|---|---|",
        ]
    )
    for record in sorted(report.records, key=lambda item: item.source.source_id):
        manifest = record.latest_manifest
        cache_summary = _cache_summary(record.cache_paths)
        lines.append(
            "| "
            f"{record.source.source_id} | "
            f"{_escape_markdown_table(record.source.provider)} | "
            f"{_source_type_label(record.source.source_type)} | "
            f"{', '.join(record.source.domains)} | "
            f"{record.health_score} | "
            f"{_format_manifest_datetime(manifest)} | "
            f"{'' if manifest is None or manifest.row_count is None else manifest.row_count} | "
            f"{_checksum_prefix(None if manifest is None else manifest.checksum_sha256)} | "
            f"{_escape_markdown_table(cache_summary)} |"
        )

    lines.extend(
        [
            "",
            "## Latest Manifest 明细",
            "",
            "| Source | Provider | Endpoint | Request parameters | Output |",
            "|---|---|---|---|---|",
        ]
    )
    manifest_records = [record for record in report.records if record.latest_manifest is not None]
    if not manifest_records:
        lines.append("| 无 |  |  |  |  |")
    else:
        for record in sorted(manifest_records, key=lambda item: item.source.source_id):
            manifest = record.latest_manifest
            assert manifest is not None
            lines.append(
                "| "
                f"{manifest.source_id} | "
                f"{_escape_markdown_table(manifest.provider)} | "
                f"{_escape_markdown_table(manifest.endpoint)} | "
                f"{_escape_markdown_table(_compact_text(manifest.request_parameters))} | "
                f"{_escape_markdown_table(manifest.output_path)} |"
            )

    lines.extend(["", "## 问题", ""])
    catalog_issues = tuple(report.catalog_report.issues)
    if not report.issues and not catalog_issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Source | Domain | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in catalog_issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.source_id or ''} | "
                "catalog | "
                f"{_escape_markdown_table(issue.message)} |"
            )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.source_id or ''} | "
                f"{issue.domain or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    return "\n".join(lines) + "\n"


def write_data_source_health_report(
    report: DataSourceHealthReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_data_source_health_report(report), encoding="utf-8")
    return output_path


def default_data_source_health_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"data_sources_health_{as_of.isoformat()}.md"


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
        "| Source | Provider | 类型 | 状态 | 领域 | Cadence | LLM 权限 | 缓存 |",
        "|---|---|---|---|---|---|---|---|",
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
            f"{_llm_permission_label(source)} | "
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


def _read_download_manifest(
    path: Path,
    issues: list[DataSourceHealthIssue],
) -> pd.DataFrame | None:
    if not path.exists():
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="download_manifest_missing",
                message=f"Download manifest 不存在：{path}",
            )
        )
        return None
    try:
        manifest = pd.read_csv(path)
    except Exception as exc:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="download_manifest_unreadable",
                message=f"Download manifest 无法读取：{exc}",
            )
        )
        return None

    missing = [
        column for column in DOWNLOAD_MANIFEST_REQUIRED_COLUMNS if column not in manifest.columns
    ]
    if missing:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="download_manifest_missing_columns",
                message=f"Download manifest 缺少字段：{', '.join(missing)}",
            )
        )
        return manifest
    return manifest


def _latest_manifest_record(
    source: DataSourceConfig,
    manifest: pd.DataFrame | None,
    issues: list[DataSourceHealthIssue],
    project_root: Path,
) -> ManifestHealthRecord | None:
    if manifest is None or "source_id" not in manifest.columns:
        return None
    rows = manifest.loc[manifest["source_id"] == source.source_id].copy()
    if rows.empty:
        if _requires_download_manifest(source):
            issues.append(
                DataSourceHealthIssue(
                    severity=DataSourceIssueSeverity.WARNING,
                    code="source_missing_download_manifest_record",
                    source_id=source.source_id,
                    message="该来源的关键缓存没有 download_manifest 记录。",
                )
            )
        return None

    rows["_downloaded_at"] = pd.to_datetime(
        rows["downloaded_at"],
        errors="coerce",
        utc=True,
    )
    rows = rows.sort_values("_downloaded_at", na_position="first")
    latest = rows.iloc[-1]
    downloaded_at = latest["_downloaded_at"]
    parsed_downloaded_at = None if pd.isna(downloaded_at) else downloaded_at.to_pydatetime()
    output_path = _resolve_path(str(latest.get("output_path", "")), project_root)
    checksum = _optional_str(latest.get("checksum_sha256"))
    row_count = _optional_int(latest.get("row_count"))
    record = ManifestHealthRecord(
        source_id=source.source_id,
        provider=_optional_str(latest.get("provider")) or "",
        endpoint=_optional_str(latest.get("endpoint")) or "",
        request_parameters=_optional_str(latest.get("request_parameters")) or "",
        downloaded_at=parsed_downloaded_at,
        output_path=str(output_path),
        row_count=row_count,
        checksum_sha256=checksum,
    )
    _check_manifest_record_consistency(source, record, output_path, issues)
    return record


def _cache_path_health(configured_path: str, project_root: Path) -> CachePathHealth:
    pattern = _template_to_glob(configured_path)
    if _has_glob(pattern):
        matches = tuple(Path(item) for item in glob(str(_resolve_path(pattern, project_root))))
        return CachePathHealth(
            configured_path=configured_path,
            resolved_path=str(_resolve_path(pattern, project_root)),
            exists=bool(matches),
            matched_path_count=len(matches),
            note="glob_pattern",
        )

    resolved = _resolve_path(configured_path, project_root)
    if not resolved.exists():
        return CachePathHealth(
            configured_path=configured_path,
            resolved_path=str(resolved),
            exists=False,
            matched_path_count=0,
        )
    if resolved.is_dir():
        matched_count = sum(1 for item in resolved.rglob("*") if item.is_file())
        return CachePathHealth(
            configured_path=configured_path,
            resolved_path=str(resolved),
            exists=True,
            matched_path_count=matched_count,
            note="directory",
        )
    return CachePathHealth(
        configured_path=configured_path,
        resolved_path=str(resolved),
        exists=True,
        matched_path_count=1,
        row_count=_csv_row_count(resolved),
        checksum_sha256=_file_sha256(resolved),
    )


def _check_source_cache_health(
    source: DataSourceConfig,
    cache_paths: tuple[CachePathHealth, ...],
    issues: list[DataSourceHealthIssue],
) -> None:
    if source.status != "active":
        return
    for cache in cache_paths:
        if cache.exists:
            continue
        severity = (
            DataSourceIssueSeverity.ERROR
            if _is_critical_cache_path(cache.configured_path)
            else DataSourceIssueSeverity.WARNING
        )
        issues.append(
            DataSourceHealthIssue(
                severity=severity,
                code="cache_path_missing",
                source_id=source.source_id,
                message=f"缓存路径不存在或无匹配文件：{cache.configured_path}",
            )
        )
    for cache in cache_paths:
        if cache.exists and cache.row_count == 0:
            issues.append(
                DataSourceHealthIssue(
                    severity=DataSourceIssueSeverity.ERROR,
                    code="cache_row_count_zero",
                    source_id=source.source_id,
                    message=f"缓存文件 row count 为 0：{cache.configured_path}",
                )
            )


def _check_source_manifest_health(
    source: DataSourceConfig,
    manifest: ManifestHealthRecord | None,
    issues: list[DataSourceHealthIssue],
    as_of: date,
) -> None:
    if manifest is None:
        return
    if manifest.row_count is None or manifest.row_count <= 0:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="manifest_row_count_invalid",
                source_id=source.source_id,
                message="Latest manifest row_count 缺失或小于等于 0。",
            )
        )
    if manifest.downloaded_at is None:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="manifest_downloaded_at_invalid",
                source_id=source.source_id,
                message="Latest manifest downloaded_at 无法解析。",
            )
        )
        return
    age_days = (as_of - manifest.downloaded_at.date()).days
    if age_days < -1:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="manifest_downloaded_at_in_future",
                source_id=source.source_id,
                message="Latest manifest downloaded_at 晚于评估日期。",
            )
        )
    threshold = _freshness_threshold_days(source.cadence)
    if threshold is not None and age_days > threshold:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="provider_cache_stale",
                source_id=source.source_id,
                message=f"Latest manifest 距评估日期 {age_days} 天，超过 {threshold} 天阈值。",
            )
        )


def _check_manifest_record_consistency(
    source: DataSourceConfig,
    record: ManifestHealthRecord,
    output_path: Path,
    issues: list[DataSourceHealthIssue],
) -> None:
    if record.provider and record.provider != source.provider:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="manifest_provider_mismatch",
                source_id=source.source_id,
                message=(
                    f"Manifest provider `{record.provider}` 与目录 "
                    f"`{source.provider}` 不一致。"
                ),
            )
        )
    if record.endpoint and source.endpoint and record.endpoint != source.endpoint:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.WARNING,
                code="manifest_endpoint_mismatch",
                source_id=source.source_id,
                message=(
                    f"Manifest endpoint `{record.endpoint}` 与目录 "
                    f"`{source.endpoint}` 不一致。"
                ),
            )
        )
    if record.request_parameters:
        try:
            json.loads(record.request_parameters)
        except json.JSONDecodeError:
            issues.append(
                DataSourceHealthIssue(
                    severity=DataSourceIssueSeverity.WARNING,
                    code="manifest_request_parameters_not_json",
                    source_id=source.source_id,
                    message="Manifest request_parameters 不是有效 JSON。",
                )
            )
    if not output_path.exists():
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="manifest_output_missing",
                source_id=source.source_id,
                message=f"Manifest output_path 不存在：{output_path}",
            )
        )
        return
    actual_checksum = _file_sha256(output_path)
    if record.checksum_sha256 and actual_checksum != record.checksum_sha256:
        issues.append(
            DataSourceHealthIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="manifest_checksum_mismatch",
                source_id=source.source_id,
                message="Manifest checksum 与当前缓存文件不一致。",
            )
        )


def _build_domain_reconciliation(
    config: DataSourcesConfig,
    issues: list[DataSourceHealthIssue],
) -> tuple[DomainReconciliationRecord, ...]:
    records: list[DomainReconciliationRecord] = []
    active_sources = [source for source in config.sources if source.status == "active"]
    for domain in RECONCILIATION_REQUIRED_DOMAINS:
        domain_sources = tuple(
            source for source in active_sources if domain in source.domains
        )
        qualified_sources = tuple(
            source
            for source in domain_sources
            if source.source_type in {"primary_source", "paid_vendor"}
        )
        status = "COVERED" if len(qualified_sources) >= 2 else "NOT_COVERED"
        records.append(
            DomainReconciliationRecord(
                domain=domain,
                status=status,
                active_source_ids=tuple(source.source_id for source in domain_sources),
                qualified_source_ids=tuple(source.source_id for source in qualified_sources),
            )
        )
        if status == "NOT_COVERED":
            issues.append(
                DataSourceHealthIssue(
                    severity=DataSourceIssueSeverity.WARNING,
                    code="reconciliation_not_covered",
                    domain=domain,
                    message="该领域少于两个 qualified source，跨供应商 reconciliation 未覆盖。",
                )
            )
    return tuple(records)


def _provider_health_score(errors: int, warnings: int) -> int:
    return max(0, 100 - errors * 30 - warnings * 8)


def _requires_download_manifest(source: DataSourceConfig) -> bool:
    return any(
        path.replace("\\", "/") in {"data/raw/prices_daily.csv", "data/raw/rates_daily.csv"}
        for path in source.cache_paths
    )


def _is_critical_cache_path(configured_path: str) -> bool:
    normalized = configured_path.replace("\\", "/")
    return normalized in {"data/raw/prices_daily.csv", "data/raw/rates_daily.csv"}


def _freshness_threshold_days(cadence: str) -> int | None:
    return {
        "daily": 3,
        "weekly": 10,
        "monthly": 45,
        "quarterly": 120,
    }.get(cadence)


def _template_to_glob(path: str) -> str:
    return (
        path.replace("YYYY-MM-DD", "*")
        .replace("YYYY_Qn", "*")
        .replace("YYYY", "*")
        .replace("Qn", "*")
    )


def _has_glob(path: str) -> bool:
    return any(char in path for char in "*?[]")


def _resolve_path(path: str, project_root: Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else project_root / candidate


def _csv_row_count(path: Path) -> int | None:
    if path.suffix.lower() != ".csv":
        return None
    try:
        return len(pd.read_csv(path))
    except Exception:
        return None


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _optional_str(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)


def _optional_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_manifest_datetime(manifest: ManifestHealthRecord | None) -> str:
    if manifest is None or manifest.downloaded_at is None:
        return ""
    return manifest.downloaded_at.isoformat()


def _checksum_prefix(value: str | None) -> str:
    if not value:
        return ""
    return value[:12]


def _cache_summary(caches: tuple[CachePathHealth, ...]) -> str:
    if not caches:
        return "未声明"
    existing = sum(1 for cache in caches if cache.exists)
    return f"{existing}/{len(caches)} paths"


def _compact_text(value: str, limit: int = 140) -> str:
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3]}..."


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


def _check_llm_permission_constraints(
    source: DataSourceConfig,
    issues: list[DataSourceIssue],
) -> None:
    permission = source.llm_permission
    if not permission.external_llm_allowed:
        return

    if not permission.approval_ref:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="external_llm_permission_missing_approval_ref",
                source_id=source.source_id,
                message="允许外部 LLM 处理的 provider 必须记录 approval_ref。",
            )
        )
    if permission.reviewed_at is None:
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="external_llm_permission_missing_reviewed_at",
                source_id=source.source_id,
                message="允许外部 LLM 处理的 provider 必须记录 reviewed_at。",
            )
        )
    if source.source_type == "paid_vendor" and permission.license_scope == "unknown":
        issues.append(
            DataSourceIssue(
                severity=DataSourceIssueSeverity.ERROR,
                code="paid_vendor_llm_permission_unknown_license_scope",
                source_id=source.source_id,
                message="付费供应商允许外部 LLM 处理前必须记录 license_scope。",
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
    if source.source_id == "sec_company_facts":
        return {"provider", "endpoint", "request_parameters", "downloaded_at", "fact_count"}
    return {"provider", "endpoint", "request_parameters", "downloaded_at", "row_count"}


def _source_type_label(source_type: str) -> str:
    return {
        "primary_source": "一手来源",
        "paid_vendor": "付费供应商",
        "public_convenience": "公开便利源",
        "manual_input": "手工输入",
    }.get(source_type, source_type)


def _llm_permission_label(source: DataSourceConfig) -> str:
    permission = source.llm_permission
    if not permission.external_llm_allowed:
        return "fail_closed"
    return f"allowed:{permission.max_content_sent_level}"


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
