from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from ai_trading_system.config import (
    FundamentalMetricConfig,
    FundamentalMetricsConfig,
    SecCompaniesConfig,
    SecCompanyConfig,
)
from ai_trading_system.fundamentals.sec_validation import SecCompanyFactsValidationReport

PeriodType = Literal["annual", "quarterly"]


class SecMetricIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class SecMetricIssue:
    severity: SecMetricIssueSeverity
    code: str
    message: str
    ticker: str | None = None
    metric_id: str | None = None
    period_type: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class SecFundamentalMetricRow:
    as_of: date
    ticker: str
    cik: str
    company_name: str
    metric_id: str
    metric_name: str
    period_type: PeriodType
    fiscal_year: int | None
    fiscal_period: str
    end_date: date | None
    filed_date: date | None
    form: str
    taxonomy: str
    concept: str
    unit: str
    value: float
    accession_number: str
    source_path: Path


@dataclass(frozen=True)
class SecFundamentalMetricsReport:
    as_of: date
    input_dir: Path
    validation_report: SecCompanyFactsValidationReport
    rows: tuple[SecFundamentalMetricRow, ...]
    issues: tuple[SecMetricIssue, ...] = field(default_factory=tuple)

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def company_count(self) -> int:
        return len({row.ticker for row in self.rows})

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == SecMetricIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == SecMetricIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.validation_report.passed and self.error_count == 0

    @property
    def status(self) -> str:
        if not self.validation_report.passed or self.error_count:
            return "FAIL"
        if self.validation_report.warning_count or self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


def build_sec_fundamental_metrics_report(
    companies: SecCompaniesConfig,
    metrics: FundamentalMetricsConfig,
    input_dir: Path,
    as_of: date,
    validation_report: SecCompanyFactsValidationReport,
) -> SecFundamentalMetricsReport:
    issues: list[SecMetricIssue] = []
    rows: list[SecFundamentalMetricRow] = []

    if not validation_report.passed:
        raise ValueError("SEC companyfacts 缓存校验必须通过后才能抽取基本面指标")

    for company in companies.companies:
        if not company.active:
            continue
        path = input_dir / f"{company.ticker.lower()}_companyfacts.json"
        data = _load_json(path, company, issues)
        if data is None:
            continue
        for metric in metrics.metrics:
            for period_type in metric.preferred_periods:
                row = _extract_metric_row(
                    company=company,
                    metric=metric,
                    period_type=period_type,
                    data=data,
                    path=path,
                    as_of=as_of,
                )
                if row is None:
                    issues.append(
                        SecMetricIssue(
                            severity=SecMetricIssueSeverity.WARNING,
                            code="sec_metric_missing",
                            ticker=company.ticker,
                            metric_id=metric.metric_id,
                            period_type=period_type,
                            path=path,
                            message="未在 SEC companyfacts 中找到该指标的可用事实。",
                        )
                    )
                    continue
                rows.append(row)

    return SecFundamentalMetricsReport(
        as_of=as_of,
        input_dir=input_dir,
        validation_report=validation_report,
        rows=tuple(rows),
        issues=tuple(issues),
    )


def write_sec_fundamental_metrics_csv(
    report: SecFundamentalMetricsReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame(
        [_row_record(row) for row in report.rows],
        columns=list(_SEC_FUNDAMENTAL_METRIC_COLUMNS),
    )
    if output_path.exists():
        existing = pd.read_csv(output_path)
        if "as_of" not in existing.columns:
            raise ValueError(f"existing SEC fundamentals file is missing as_of: {output_path}")
        existing = existing.loc[existing["as_of"] != report.as_of.isoformat()]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def render_sec_fundamental_metrics_report(
    report: SecFundamentalMetricsReport,
    validation_report_path: Path,
    output_csv_path: Path,
) -> str:
    lines = [
        "# SEC 基本面指标摘要",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入目录：`{report.input_dir}`",
        f"- SEC 缓存校验状态：{report.validation_report.status}",
        f"- SEC 缓存校验报告：`{validation_report_path}`",
        f"- 指标 CSV：`{output_csv_path}`",
        f"- 公司数：{report.company_count}",
        f"- 指标行数：{report.row_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 指标摘要",
        "",
    ]

    if not report.rows:
        lines.append("未抽取到任何 SEC 基本面指标。")
    else:
        lines.extend(
            [
                "| Ticker | 指标 | 周期 | 财年 | 财期 | 截止日 | 披露日 | 表格 | 数值 | 来源概念 |",
                "|---|---|---|---:|---|---|---|---|---:|---|",
            ]
        )
        for row in sorted(
            report.rows,
            key=lambda item: (item.ticker, item.metric_id, item.period_type),
        ):
            end_date = row.end_date.isoformat() if row.end_date else ""
            filed_date = row.filed_date.isoformat() if row.filed_date else ""
            lines.append(
                "| "
                f"{row.ticker} | "
                f"{_escape_markdown_table(row.metric_name)} | "
                f"{_period_label(row.period_type)} | "
                f"{row.fiscal_year or ''} | "
                f"{row.fiscal_period} | "
                f"{end_date} | "
                f"{filed_date} | "
                f"{row.form} | "
                f"{row.value:.2f} | "
                f"{row.taxonomy}:{row.concept} ({row.unit}) |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Ticker | 指标 | 周期 | 文件 | 说明 |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.metric_id or ''} | "
                f"{issue.period_type or ''} | "
                f"{_escape_markdown_table(str(issue.path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本报告只把 SEC companyfacts 原始 JSON 抽成结构化摘要，不直接进入自动评分。",
            "- 指标映射来自 `config/fundamental_metrics.yaml`，"
            "taxonomy 差异和缺失项必须先人工复核。",
            "- 年度指标优先使用 FY 事实；季度指标优先使用 Q1/Q2/Q3/Q4 事实，"
            "均选最新截止日和披露日。",
            "- 数值保留 SEC companyfacts 原始符号，不在抽取层重写正负号。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_sec_fundamental_metrics_report(
    report: SecFundamentalMetricsReport,
    validation_report_path: Path,
    output_csv_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_sec_fundamental_metrics_report(
            report,
            validation_report_path=validation_report_path,
            output_csv_path=output_csv_path,
        ),
        encoding="utf-8",
    )
    return output_path


def default_sec_fundamental_metrics_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"sec_fundamentals_{as_of.isoformat()}.md"


def default_sec_fundamental_metrics_csv_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"sec_fundamentals_{as_of.isoformat()}.csv"


_SEC_FUNDAMENTAL_METRIC_COLUMNS = (
    "as_of",
    "ticker",
    "cik",
    "company_name",
    "metric_id",
    "metric_name",
    "period_type",
    "fiscal_year",
    "fiscal_period",
    "end_date",
    "filed_date",
    "form",
    "taxonomy",
    "concept",
    "unit",
    "value",
    "accession_number",
    "source_path",
)


def _load_json(
    path: Path,
    company: SecCompanyConfig,
    issues: list[SecMetricIssue],
) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.ERROR,
                code="sec_companyfacts_json_unreadable",
                ticker=company.ticker,
                path=path,
                message=f"SEC companyfacts JSON 无法读取：{exc}",
            )
        )
        return None
    if not isinstance(raw, dict):
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.ERROR,
                code="sec_companyfacts_json_not_object",
                ticker=company.ticker,
                path=path,
                message="SEC companyfacts JSON 顶层结构不是 object。",
            )
        )
        return None
    return raw


def _extract_metric_row(
    company: SecCompanyConfig,
    metric: FundamentalMetricConfig,
    period_type: PeriodType,
    data: dict[str, Any],
    path: Path,
    as_of: date,
) -> SecFundamentalMetricRow | None:
    for concept_config in metric.concepts:
        fact = _latest_fact(
            data,
            concept_config.taxonomy,
            concept_config.concept,
            concept_config.unit,
            period_type,
        )
        if fact is None:
            continue
        value = _numeric_value(fact.get("val"))
        if value is None:
            continue
        return SecFundamentalMetricRow(
            as_of=as_of,
            ticker=company.ticker,
            cik=company.cik,
            company_name=company.company_name,
            metric_id=metric.metric_id,
            metric_name=metric.name,
            period_type=period_type,
            fiscal_year=_int_or_none(fact.get("fy")),
            fiscal_period=str(fact.get("fp") or ""),
            end_date=_date_or_none(fact.get("end")),
            filed_date=_date_or_none(fact.get("filed")),
            form=str(fact.get("form") or ""),
            taxonomy=concept_config.taxonomy,
            concept=concept_config.concept,
            unit=concept_config.unit,
            value=value,
            accession_number=str(fact.get("accn") or ""),
            source_path=path,
        )
    return None


def _latest_fact(
    data: dict[str, Any],
    taxonomy: str,
    concept: str,
    unit: str,
    period_type: PeriodType,
) -> dict[str, Any] | None:
    facts = data.get("facts")
    if not isinstance(facts, dict):
        return None
    taxonomy_facts = facts.get(taxonomy)
    if not isinstance(taxonomy_facts, dict):
        return None
    concept_facts = taxonomy_facts.get(concept)
    if not isinstance(concept_facts, dict):
        return None
    units = concept_facts.get("units")
    if not isinstance(units, dict):
        return None
    unit_facts = units.get(unit)
    if not isinstance(unit_facts, list):
        return None

    candidates = [
        fact
        for fact in unit_facts
        if isinstance(fact, dict)
        and _numeric_value(fact.get("val")) is not None
        and _period_matches(fact, period_type)
    ]
    if not candidates:
        return None
    return max(candidates, key=_fact_sort_key)


def _period_matches(fact: dict[str, Any], period_type: PeriodType) -> bool:
    fiscal_period = str(fact.get("fp") or "").upper()
    form = str(fact.get("form") or "").upper()
    if period_type == "annual":
        return fiscal_period == "FY" or form in {"10-K", "20-F", "40-F"}
    return fiscal_period in {"Q1", "Q2", "Q3", "Q4"} and form not in {"10-K", "20-F", "40-F"}


def _fact_sort_key(fact: dict[str, Any]) -> tuple[date, date, int]:
    return (
        _date_or_none(fact.get("end")) or date.min,
        _date_or_none(fact.get("filed")) or date.min,
        _int_or_none(fact.get("fy")) or 0,
    )


def _row_record(row: SecFundamentalMetricRow) -> dict[str, object]:
    return {
        "as_of": row.as_of.isoformat(),
        "ticker": row.ticker,
        "cik": row.cik,
        "company_name": row.company_name,
        "metric_id": row.metric_id,
        "metric_name": row.metric_name,
        "period_type": row.period_type,
        "fiscal_year": row.fiscal_year,
        "fiscal_period": row.fiscal_period,
        "end_date": row.end_date.isoformat() if row.end_date else "",
        "filed_date": row.filed_date.isoformat() if row.filed_date else "",
        "form": row.form,
        "taxonomy": row.taxonomy,
        "concept": row.concept,
        "unit": row.unit,
        "value": row.value,
        "accession_number": row.accession_number,
        "source_path": str(row.source_path),
    }


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date_or_none(value: Any) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _period_label(period_type: str) -> str:
    return {"annual": "年度", "quarterly": "季度"}.get(period_type, period_type)


def _severity_label(severity: SecMetricIssueSeverity) -> str:
    if severity == SecMetricIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
