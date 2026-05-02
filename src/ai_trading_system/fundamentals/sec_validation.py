from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig


class SecCompanyFactsIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class SecCompanyFactsIssue:
    severity: SecCompanyFactsIssueSeverity
    code: str
    message: str
    ticker: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class SecCompanyFactsFileSummary:
    ticker: str
    cik: str
    path: Path
    exists: bool
    facts_count: int = 0
    taxonomy_count: int = 0
    checksum_sha256: str | None = None


@dataclass(frozen=True)
class SecCompanyFactsValidationReport:
    as_of: date
    input_dir: Path
    manifest_path: Path
    files: tuple[SecCompanyFactsFileSummary, ...]
    issues: tuple[SecCompanyFactsIssue, ...] = field(default_factory=tuple)

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def available_count(self) -> int:
        return sum(1 for file in self.files if file.exists)

    @property
    def error_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == SecCompanyFactsIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == SecCompanyFactsIssueSeverity.WARNING
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


def validate_sec_companyfacts_cache(
    config: SecCompaniesConfig,
    input_dir: Path,
    as_of: date,
) -> SecCompanyFactsValidationReport:
    manifest_path = input_dir / "sec_companyfacts_manifest.csv"
    manifest = _read_manifest(manifest_path)
    issues: list[SecCompanyFactsIssue] = []
    summaries: list[SecCompanyFactsFileSummary] = []

    if manifest is None:
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.WARNING,
                code="sec_companyfacts_manifest_missing",
                path=manifest_path,
                message="SEC companyfacts 下载审计清单不存在；请先运行 download-sec-companyfacts。",
            )
        )

    for company in config.companies:
        if not company.active:
            continue
        summary = _validate_company_file(company, input_dir, manifest, issues)
        summaries.append(summary)

    return SecCompanyFactsValidationReport(
        as_of=as_of,
        input_dir=input_dir,
        manifest_path=manifest_path,
        files=tuple(summaries),
        issues=tuple(issues),
    )


def render_sec_companyfacts_validation_report(
    report: SecCompanyFactsValidationReport,
) -> str:
    lines = [
        "# SEC Company Facts 缓存校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入目录：`{report.input_dir}`",
        f"- 下载审计清单：`{report.manifest_path}`",
        f"- 配置公司数：{report.file_count}",
        f"- 已缓存公司数：{report.available_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 文件概览",
        "",
        "| Ticker | CIK | 存在 | Facts | Taxonomies | sha256 | 文件 |",
        "|---|---|---|---:|---:|---|---|",
    ]

    for item in sorted(report.files, key=lambda file: file.ticker):
        lines.append(
            "| "
            f"{item.ticker} | "
            f"{item.cik} | "
            f"{'是' if item.exists else '否'} | "
            f"{item.facts_count} | "
            f"{item.taxonomy_count} | "
            f"{item.checksum_sha256 or ''} | "
            f"`{item.path}` |"
        )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Ticker | 文件 | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{_escape_markdown_table(str(issue.path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本报告只校验 SEC companyfacts 原始缓存和下载审计清单，不等同于基本面评分。",
            "- CIK、taxonomy 和 checksum 通过前，不应从该缓存抽取自动评分指标。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_sec_companyfacts_validation_report(
    report: SecCompanyFactsValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_sec_companyfacts_validation_report(report), encoding="utf-8")
    return output_path


def default_sec_companyfacts_validation_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"sec_companyfacts_validation_{as_of.isoformat()}.md"


def _validate_company_file(
    company: SecCompanyConfig,
    input_dir: Path,
    manifest: pd.DataFrame | None,
    issues: list[SecCompanyFactsIssue],
) -> SecCompanyFactsFileSummary:
    path = input_dir / f"{company.ticker.lower()}_companyfacts.json"
    if not path.exists():
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.ERROR,
                code="sec_companyfacts_file_missing",
                ticker=company.ticker,
                path=path,
                message="SEC companyfacts JSON 不存在；请先下载该公司缓存。",
            )
        )
        return SecCompanyFactsFileSummary(
            ticker=company.ticker,
            cik=company.cik,
            path=path,
            exists=False,
        )

    checksum = _sha256_file(path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.ERROR,
                code="sec_companyfacts_json_unreadable",
                ticker=company.ticker,
                path=path,
                message=f"SEC companyfacts JSON 无法读取或解析：{exc}",
            )
        )
        return SecCompanyFactsFileSummary(
            ticker=company.ticker,
            cik=company.cik,
            path=path,
            exists=True,
            checksum_sha256=checksum,
        )

    facts = data.get("facts") if isinstance(data, dict) else None
    taxonomy_count = len(facts) if isinstance(facts, dict) else 0
    facts_count = _count_company_facts(data) if isinstance(data, dict) else 0
    _check_company_payload(company, data, facts, path, issues)
    _check_manifest_checksum(company, manifest, checksum, path, issues)

    return SecCompanyFactsFileSummary(
        ticker=company.ticker,
        cik=company.cik,
        path=path,
        exists=True,
        facts_count=facts_count,
        taxonomy_count=taxonomy_count,
        checksum_sha256=checksum,
    )


def _check_company_payload(
    company: SecCompanyConfig,
    data: Any,
    facts: Any,
    path: Path,
    issues: list[SecCompanyFactsIssue],
) -> None:
    if not isinstance(data, dict):
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.ERROR,
                code="sec_companyfacts_json_not_object",
                ticker=company.ticker,
                path=path,
                message="SEC companyfacts JSON 顶层结构不是 object。",
            )
        )
        return

    payload_cik = str(data.get("cik", "")).zfill(10)
    if payload_cik != company.cik:
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.ERROR,
                code="sec_companyfacts_cik_mismatch",
                ticker=company.ticker,
                path=path,
                message=f"JSON CIK {payload_cik} 与配置 CIK {company.cik} 不一致。",
            )
        )

    if not isinstance(facts, dict) or not facts:
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.ERROR,
                code="sec_companyfacts_missing_facts",
                ticker=company.ticker,
                path=path,
                message="SEC companyfacts JSON 缺少 facts 数据。",
            )
        )
        return

    missing_taxonomies = [
        taxonomy for taxonomy in company.expected_taxonomies if taxonomy not in facts
    ]
    if missing_taxonomies:
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.ERROR,
                code="sec_companyfacts_missing_expected_taxonomy",
                ticker=company.ticker,
                path=path,
                message=f"缺少预期 taxonomy：{', '.join(missing_taxonomies)}",
            )
        )


def _check_manifest_checksum(
    company: SecCompanyConfig,
    manifest: pd.DataFrame | None,
    checksum: str,
    path: Path,
    issues: list[SecCompanyFactsIssue],
) -> None:
    if manifest is None:
        return

    required_columns = {"ticker", "cik", "checksum_sha256"}
    missing_columns = sorted(required_columns - set(manifest.columns))
    if missing_columns:
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.WARNING,
                code="sec_companyfacts_manifest_missing_columns",
                ticker=company.ticker,
                path=path,
                message=f"下载审计清单缺少字段：{', '.join(missing_columns)}",
            )
        )
        return

    matches = (
        (manifest["ticker"].astype(str).str.upper() == company.ticker)
        & (manifest["cik"].astype(str).str.zfill(10) == company.cik)
        & (manifest["checksum_sha256"].astype(str) == checksum)
    )
    if not matches.any():
        issues.append(
            SecCompanyFactsIssue(
                severity=SecCompanyFactsIssueSeverity.WARNING,
                code="sec_companyfacts_manifest_checksum_missing",
                ticker=company.ticker,
                path=path,
                message="当前 JSON checksum 未出现在下载审计清单中；请确认缓存来源。",
            )
        )


def _read_manifest(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _count_company_facts(data: dict[str, Any]) -> int:
    facts = data.get("facts")
    if not isinstance(facts, dict):
        return 0

    total = 0
    for taxonomy_value in facts.values():
        if not isinstance(taxonomy_value, dict):
            continue
        for concept_value in taxonomy_value.values():
            if not isinstance(concept_value, dict):
                continue
            units = concept_value.get("units")
            if not isinstance(units, dict):
                continue
            for unit_facts in units.values():
                if isinstance(unit_facts, list):
                    total += len(unit_facts)
    return total


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _severity_label(severity: SecCompanyFactsIssueSeverity) -> str:
    if severity == SecCompanyFactsIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
