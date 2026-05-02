from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from numbers import Real
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from ai_trading_system.config import (
    FundamentalDerivedMetricConfig,
    FundamentalMetricConfig,
    FundamentalMetricsConfig,
    SecCompaniesConfig,
    SecCompanyConfig,
)
from ai_trading_system.fundamentals.sec_validation import SecCompanyFactsValidationReport

PeriodType = Literal["annual", "quarterly"]
_ANNUAL_FORMS = {"10-K", "20-F", "40-F"}
_ANNUAL_FRAME_PATTERN = re.compile(r"^CY\d{4}$")
_QUARTER_FRAME_PATTERN = re.compile(r"^CY\d{4}Q[1-4]$")


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


@dataclass(frozen=True)
class SecFundamentalMetricsCsvValidationReport:
    as_of: date
    input_path: Path
    row_count: int
    as_of_row_count: int
    expected_observation_count: int
    observed_observation_count: int
    issues: tuple[SecMetricIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == SecMetricIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == SecMetricIssueSeverity.WARNING)

    @property
    def coverage(self) -> float:
        if self.expected_observation_count == 0:
            return 0.0
        return self.observed_observation_count / self.expected_observation_count

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


@dataclass(frozen=True)
class _MetricFactCandidate:
    fact: dict[str, Any]
    taxonomy: str
    concept: str
    unit: str
    concept_priority: int


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

    all_source_metrics = [*metrics.metrics, *metrics.supporting_metrics]
    source_metrics_by_id = {metric.metric_id: metric for metric in all_source_metrics}
    derived_metrics_by_id = {
        derived_metric.metric_id: derived_metric
        for derived_metric in metrics.derived_metrics
    }

    for company in companies.companies:
        if not company.active:
            continue
        path = input_dir / f"{company.ticker.lower()}_companyfacts.json"
        data = _load_json(path, company, issues)
        if data is None:
            continue

        extracted_rows: dict[tuple[str, PeriodType], SecFundamentalMetricRow] = {}
        for source_metric in all_source_metrics:
            for period_type in _metric_periods_for_company(company, source_metric):
                row = _extract_metric_row(
                    company=company,
                    metric=source_metric,
                    period_type=period_type,
                    data=data,
                    path=path,
                    as_of=as_of,
                )
                if row is not None:
                    extracted_rows[(source_metric.metric_id, period_type)] = row

        for metric in metrics.metrics:
            for period_type in _metric_periods_for_company(company, metric):
                row = extracted_rows.get((metric.metric_id, period_type))
                if row is None:
                    row = _derive_metric_row(
                        company=company,
                        metric=metric,
                        derived_metric=derived_metrics_by_id.get(metric.metric_id),
                        period_type=period_type,
                        path=path,
                        as_of=as_of,
                        extracted_rows=extracted_rows,
                        source_metrics_by_id=source_metrics_by_id,
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
        columns=list(SEC_FUNDAMENTAL_METRIC_COLUMNS),
    )
    if output_path.exists():
        existing = pd.read_csv(output_path)
        if "as_of" not in existing.columns:
            raise ValueError(f"existing SEC fundamentals file is missing as_of: {output_path}")
        existing = existing.loc[existing["as_of"] != report.as_of.isoformat()]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def sec_fundamental_metric_rows_to_frame(
    rows: tuple[SecFundamentalMetricRow, ...],
) -> pd.DataFrame:
    return pd.DataFrame(
        [_row_record(row) for row in rows],
        columns=list(SEC_FUNDAMENTAL_METRIC_COLUMNS),
    )


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
            source_label = f"{row.taxonomy}:{row.concept} ({row.unit})"
            if row.taxonomy == "derived":
                source_label = f"{row.concept} ({row.unit})"
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
                f"{_escape_markdown_table(source_label)} |"
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
            "- 派生指标必须在 `config/fundamental_metrics.yaml` 显式声明，并且只在组件指标周期、"
            "单位、截止日、财年、财期和 accession number 一致时生成。",
            "- `config/sec_companies.yaml` 可声明 SEC companyfacts 对单个公司的可用指标周期；"
            "未声明季度覆盖的公司不会被要求从 SEC companyfacts 生成季度指标。",
            "- 年度指标只抽取年度持续期事实，排除 10-K 中披露的历史季度 frame。",
            "- 季度指标只抽取单季事实，优先使用 SEC frame 或持续期判断排除 YTD 累计数。",
            "- 同一指标会跨所有候选 taxonomy/concept/unit 选择最新可用事实，"
            "同日事实再按配置顺序打破平局。",
            "- 只抽取 `filed` 日期不晚于评估日期的事实，避免历史回测和历史日报出现未来函数。",
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


def validate_sec_fundamental_metrics_csv(
    companies: SecCompaniesConfig,
    metrics: FundamentalMetricsConfig,
    input_path: Path,
    as_of: date,
) -> SecFundamentalMetricsCsvValidationReport:
    issues: list[SecMetricIssue] = []
    if not input_path.exists():
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.ERROR,
                code="sec_fundamental_metrics_file_missing",
                path=input_path,
                message="SEC 基本面指标 CSV 不存在；请先运行 extract-sec-metrics。",
            )
        )
        return _csv_validation_report(
            as_of=as_of,
            input_path=input_path,
            row_count=0,
            as_of_row_count=0,
            companies=companies,
            metrics=metrics,
            observed_keys=set(),
            issues=issues,
        )

    try:
        frame = pd.read_csv(input_path)
    except Exception as exc:
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.ERROR,
                code="sec_fundamental_metrics_csv_unreadable",
                path=input_path,
                message=f"SEC 基本面指标 CSV 无法读取：{exc}",
            )
        )
        return _csv_validation_report(
            as_of=as_of,
            input_path=input_path,
            row_count=0,
            as_of_row_count=0,
            companies=companies,
            metrics=metrics,
            observed_keys=set(),
            issues=issues,
        )

    return _validate_sec_fundamental_metrics_frame(
        companies=companies,
        metrics=metrics,
        input_path=input_path,
        frame=frame,
        as_of=as_of,
        issues=issues,
    )


def validate_sec_fundamental_metric_rows(
    companies: SecCompaniesConfig,
    metrics: FundamentalMetricsConfig,
    rows: tuple[SecFundamentalMetricRow, ...],
    source_path: Path,
    as_of: date,
) -> SecFundamentalMetricsCsvValidationReport:
    return _validate_sec_fundamental_metrics_frame(
        companies=companies,
        metrics=metrics,
        input_path=source_path,
        frame=sec_fundamental_metric_rows_to_frame(rows),
        as_of=as_of,
        issues=[],
    )


def _validate_sec_fundamental_metrics_frame(
    companies: SecCompaniesConfig,
    metrics: FundamentalMetricsConfig,
    input_path: Path,
    frame: pd.DataFrame,
    as_of: date,
    issues: list[SecMetricIssue],
) -> SecFundamentalMetricsCsvValidationReport:
    missing_columns = sorted(set(SEC_FUNDAMENTAL_METRIC_COLUMNS) - set(frame.columns))
    if missing_columns:
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.ERROR,
                code="sec_fundamental_metrics_missing_columns",
                path=input_path,
                message=f"SEC 基本面指标 CSV 缺少字段：{', '.join(missing_columns)}。",
            )
        )
        return _csv_validation_report(
            as_of=as_of,
            input_path=input_path,
            row_count=len(frame),
            as_of_row_count=0,
            companies=companies,
            metrics=metrics,
            observed_keys=set(),
            issues=issues,
        )

    as_of_frame = frame.loc[frame["as_of"].astype(str) == as_of.isoformat()].copy()
    if as_of_frame.empty:
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.ERROR,
                code="sec_fundamental_metrics_as_of_missing",
                path=input_path,
                message=f"SEC 基本面指标 CSV 不包含评估日期 {as_of.isoformat()} 的记录。",
            )
        )

    _validate_csv_duplicate_keys(as_of_frame, input_path, issues)
    _validate_csv_values(as_of_frame, input_path, as_of, issues)
    observed_keys = _observed_metric_keys(as_of_frame)
    expected_keys = _expected_metric_keys(companies, metrics)
    missing_keys = sorted(expected_keys - observed_keys)
    if observed_keys.isdisjoint(expected_keys) and expected_keys:
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.ERROR,
                code="sec_fundamental_metrics_no_expected_metrics",
                path=input_path,
                message="SEC 基本面指标 CSV 没有覆盖任何当前配置要求的指标。",
            )
        )
    elif missing_keys:
        missing_preview = ", ".join(
            f"{ticker}:{metric_id}:{period_type}"
            for ticker, metric_id, period_type in missing_keys[:12]
        )
        if len(missing_keys) > 12:
            missing_preview += f" 等 {len(missing_keys)} 项"
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.WARNING,
                code="sec_fundamental_metrics_coverage_incomplete",
                path=input_path,
                message=f"SEC 基本面指标覆盖不完整：{missing_preview}。",
            )
        )

    return _csv_validation_report(
        as_of=as_of,
        input_path=input_path,
        row_count=len(frame),
        as_of_row_count=len(as_of_frame),
        companies=companies,
        metrics=metrics,
        observed_keys=observed_keys,
        issues=issues,
    )


def render_sec_fundamental_metrics_validation_report(
    report: SecFundamentalMetricsCsvValidationReport,
) -> str:
    lines = [
        "# SEC 基本面指标 CSV 校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入文件：`{report.input_path}`",
        f"- 总行数：{report.row_count}",
        f"- 当日行数：{report.as_of_row_count}",
        f"- 预期观测数：{report.expected_observation_count}",
        f"- 已覆盖观测数：{report.observed_observation_count}",
        f"- 覆盖率：{report.coverage:.0%}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 问题",
        "",
    ]
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
            "- 本校验只检查已抽取的 SEC 基本面指标 CSV，不替代原始 companyfacts 缓存校验。",
            "- 进入评分前仍需保证指标 CSV 来自 `extract-sec-metrics`，且原始 SEC 缓存校验已通过。",
            "- `filed_date` 晚于评估日期会被视为错误，避免历史报告和回测读取未来披露。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_sec_fundamental_metrics_validation_report(
    report: SecFundamentalMetricsCsvValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_sec_fundamental_metrics_validation_report(report),
        encoding="utf-8",
    )
    return output_path


def default_sec_fundamental_metrics_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"sec_fundamentals_{as_of.isoformat()}.md"


def default_sec_fundamental_metrics_csv_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"sec_fundamentals_{as_of.isoformat()}.csv"


def default_sec_fundamental_metrics_validation_report_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"sec_fundamentals_validation_{as_of.isoformat()}.md"


SEC_FUNDAMENTAL_METRIC_COLUMNS = (
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


def _csv_validation_report(
    as_of: date,
    input_path: Path,
    row_count: int,
    as_of_row_count: int,
    companies: SecCompaniesConfig,
    metrics: FundamentalMetricsConfig,
    observed_keys: set[tuple[str, str, str]],
    issues: list[SecMetricIssue],
) -> SecFundamentalMetricsCsvValidationReport:
    expected_keys = _expected_metric_keys(companies, metrics)
    return SecFundamentalMetricsCsvValidationReport(
        as_of=as_of,
        input_path=input_path,
        row_count=row_count,
        as_of_row_count=as_of_row_count,
        expected_observation_count=len(expected_keys),
        observed_observation_count=len(observed_keys & expected_keys),
        issues=tuple(issues),
    )


def _expected_metric_keys(
    companies: SecCompaniesConfig,
    metrics: FundamentalMetricsConfig,
) -> set[tuple[str, str, str]]:
    return {
        (company.ticker, metric.metric_id, period_type)
        for company in companies.companies
        if company.active
        for metric in metrics.metrics
        for period_type in _metric_periods_for_company(company, metric)
    }


def _metric_periods_for_company(
    company: SecCompanyConfig,
    metric: FundamentalMetricConfig,
) -> list[PeriodType]:
    supported_periods = set(company.sec_metric_periods)
    return [
        period_type
        for period_type in metric.preferred_periods
        if period_type in supported_periods
    ]


def _observed_metric_keys(frame: pd.DataFrame) -> set[tuple[str, str, str]]:
    if frame.empty:
        return set()
    return {
        (
            str(record.get("ticker", "")).upper(),
            str(record.get("metric_id", "")),
            str(record.get("period_type", "")),
        )
        for record in frame.to_dict(orient="records")
    }


def _validate_csv_duplicate_keys(
    frame: pd.DataFrame,
    input_path: Path,
    issues: list[SecMetricIssue],
) -> None:
    if frame.empty:
        return
    key_columns = ["as_of", "ticker", "metric_id", "period_type"]
    duplicate_count = int(frame.duplicated(subset=key_columns, keep=False).sum())
    if duplicate_count:
        issues.append(
            SecMetricIssue(
                severity=SecMetricIssueSeverity.ERROR,
                code="sec_fundamental_metrics_duplicate_keys",
                path=input_path,
                message=(
                    "SEC 基本面指标 CSV 存在重复 as_of/ticker/metric_id/period_type "
                    f"组合：{duplicate_count} 行。"
                ),
            )
        )


def _validate_csv_values(
    frame: pd.DataFrame,
    input_path: Path,
    as_of: date,
    issues: list[SecMetricIssue],
) -> None:
    for record in frame.to_dict(orient="records"):
        ticker = str(record.get("ticker", "")).upper()
        metric_id = str(record.get("metric_id", ""))
        period_type = str(record.get("period_type", ""))
        value = _numeric_value(record.get("value"))
        if value is None:
            issues.append(
                SecMetricIssue(
                    severity=SecMetricIssueSeverity.ERROR,
                    code="sec_fundamental_metrics_value_invalid",
                    ticker=ticker,
                    metric_id=metric_id,
                    period_type=period_type,
                    path=input_path,
                    message="SEC 基本面指标 value 不是有效数值。",
                )
            )
        elif metric_id == "revenue" and value <= 0:
            issues.append(
                SecMetricIssue(
                    severity=SecMetricIssueSeverity.ERROR,
                    code="sec_fundamental_metrics_revenue_non_positive",
                    ticker=ticker,
                    metric_id=metric_id,
                    period_type=period_type,
                    path=input_path,
                    message="SEC 基本面收入指标必须为正数。",
                )
            )

        filed_date = _date_or_none(record.get("filed_date"))
        if filed_date is None:
            issues.append(
                SecMetricIssue(
                    severity=SecMetricIssueSeverity.ERROR,
                    code="sec_fundamental_metrics_filed_date_missing",
                    ticker=ticker,
                    metric_id=metric_id,
                    period_type=period_type,
                    path=input_path,
                    message="SEC 基本面指标缺少 filed_date，无法确认是否存在未来函数。",
                )
            )
        elif filed_date > as_of:
            issues.append(
                SecMetricIssue(
                    severity=SecMetricIssueSeverity.ERROR,
                    code="sec_fundamental_metrics_filed_date_after_as_of",
                    ticker=ticker,
                    metric_id=metric_id,
                    period_type=period_type,
                    path=input_path,
                    message=(
                        f"SEC 基本面指标 filed_date={filed_date.isoformat()} "
                        f"晚于评估日期 {as_of.isoformat()}。"
                    ),
                )
            )


def _derive_metric_row(
    company: SecCompanyConfig,
    metric: FundamentalMetricConfig,
    derived_metric: FundamentalDerivedMetricConfig | None,
    period_type: PeriodType,
    path: Path,
    as_of: date,
    extracted_rows: dict[tuple[str, PeriodType], SecFundamentalMetricRow],
    source_metrics_by_id: dict[str, FundamentalMetricConfig],
) -> SecFundamentalMetricRow | None:
    if derived_metric is None:
        return None
    if (
        derived_metric.minuend_metric_id not in source_metrics_by_id
        or derived_metric.subtrahend_metric_id not in source_metrics_by_id
    ):
        return None

    minuend_row = extracted_rows.get((derived_metric.minuend_metric_id, period_type))
    subtrahend_row = extracted_rows.get(
        (derived_metric.subtrahend_metric_id, period_type)
    )
    if minuend_row is None or subtrahend_row is None:
        return None
    if not _rows_can_derive_difference(minuend_row, subtrahend_row):
        return None

    return SecFundamentalMetricRow(
        as_of=as_of,
        ticker=company.ticker,
        cik=company.cik,
        company_name=company.company_name,
        metric_id=metric.metric_id,
        metric_name=metric.name,
        period_type=period_type,
        fiscal_year=minuend_row.fiscal_year,
        fiscal_period=minuend_row.fiscal_period,
        end_date=minuend_row.end_date,
        filed_date=minuend_row.filed_date,
        form=minuend_row.form,
        taxonomy="derived",
        concept=(
            "derived:"
            f"{derived_metric.minuend_metric_id}-{derived_metric.subtrahend_metric_id}"
        ),
        unit=minuend_row.unit,
        value=minuend_row.value - subtrahend_row.value,
        accession_number=minuend_row.accession_number,
        source_path=path,
    )


def _rows_can_derive_difference(
    minuend_row: SecFundamentalMetricRow,
    subtrahend_row: SecFundamentalMetricRow,
) -> bool:
    return (
        minuend_row.period_type == subtrahend_row.period_type
        and minuend_row.fiscal_year == subtrahend_row.fiscal_year
        and minuend_row.fiscal_period == subtrahend_row.fiscal_period
        and minuend_row.end_date == subtrahend_row.end_date
        and minuend_row.filed_date == subtrahend_row.filed_date
        and minuend_row.form == subtrahend_row.form
        and minuend_row.unit == subtrahend_row.unit
        and bool(minuend_row.accession_number)
        and minuend_row.accession_number == subtrahend_row.accession_number
    )


def _extract_metric_row(
    company: SecCompanyConfig,
    metric: FundamentalMetricConfig,
    period_type: PeriodType,
    data: dict[str, Any],
    path: Path,
    as_of: date,
) -> SecFundamentalMetricRow | None:
    candidates: list[_MetricFactCandidate] = []
    for concept_priority, concept_config in enumerate(metric.concepts):
        candidates.extend(
            _candidate_facts(
                data,
                concept_config.taxonomy,
                concept_config.concept,
                concept_config.unit,
                period_type,
                as_of,
                concept_priority,
            )
        )

    if not candidates:
        return None

    selected = max(candidates, key=_candidate_sort_key)
    value = _numeric_value(selected.fact.get("val"))
    if value is None:
        return None
    return SecFundamentalMetricRow(
        as_of=as_of,
        ticker=company.ticker,
        cik=company.cik,
        company_name=company.company_name,
        metric_id=metric.metric_id,
        metric_name=metric.name,
        period_type=period_type,
        fiscal_year=_int_or_none(selected.fact.get("fy")),
        fiscal_period=str(selected.fact.get("fp") or ""),
        end_date=_date_or_none(selected.fact.get("end")),
        filed_date=_date_or_none(selected.fact.get("filed")),
        form=str(selected.fact.get("form") or ""),
        taxonomy=selected.taxonomy,
        concept=selected.concept,
        unit=selected.unit,
        value=value,
        accession_number=str(selected.fact.get("accn") or ""),
        source_path=path,
    )


def _candidate_facts(
    data: dict[str, Any],
    taxonomy: str,
    concept: str,
    unit: str,
    period_type: PeriodType,
    as_of: date,
    concept_priority: int,
) -> list[_MetricFactCandidate]:
    facts = data.get("facts")
    if not isinstance(facts, dict):
        return []
    taxonomy_facts = facts.get(taxonomy)
    if not isinstance(taxonomy_facts, dict):
        return []
    concept_facts = taxonomy_facts.get(concept)
    if not isinstance(concept_facts, dict):
        return []
    units = concept_facts.get("units")
    if not isinstance(units, dict):
        return []
    unit_facts = units.get(unit)
    if not isinstance(unit_facts, list):
        return []

    return [
        _MetricFactCandidate(
            fact=fact,
            taxonomy=taxonomy,
            concept=concept,
            unit=unit,
            concept_priority=concept_priority,
        )
        for fact in unit_facts
        if isinstance(fact, dict)
        and _numeric_value(fact.get("val")) is not None
        and _period_matches(fact, period_type)
        and _is_available_as_of(fact, as_of)
    ]


def _period_matches(fact: dict[str, Any], period_type: PeriodType) -> bool:
    fiscal_period = str(fact.get("fp") or "").upper()
    form = str(fact.get("form") or "").upper()
    if period_type == "annual":
        return (fiscal_period == "FY" or form in _ANNUAL_FORMS) and _is_annual_fact(fact)
    return (
        fiscal_period in {"Q1", "Q2", "Q3", "Q4"}
        and form not in _ANNUAL_FORMS
        and _is_single_quarter_fact(fact)
    )


def _is_annual_fact(fact: dict[str, Any]) -> bool:
    frame = str(fact.get("frame") or "").upper()
    if frame:
        return bool(_ANNUAL_FRAME_PATTERN.fullmatch(frame))

    duration_days = _duration_days(fact)
    if duration_days is None:
        return True
    return duration_days >= 300


def _is_single_quarter_fact(fact: dict[str, Any]) -> bool:
    frame = str(fact.get("frame") or "").upper()
    if frame:
        return bool(_QUARTER_FRAME_PATTERN.fullmatch(frame))

    duration_days = _duration_days(fact)
    if duration_days is None:
        return False
    return 60 <= duration_days <= 120


def _duration_days(fact: dict[str, Any]) -> int | None:
    start_date = _date_or_none(fact.get("start"))
    end_date = _date_or_none(fact.get("end"))
    if start_date is None or end_date is None or end_date < start_date:
        return None
    return (end_date - start_date).days + 1


def _is_available_as_of(fact: dict[str, Any], as_of: date) -> bool:
    filed_date = _date_or_none(fact.get("filed"))
    return filed_date is not None and filed_date <= as_of


def _candidate_sort_key(candidate: _MetricFactCandidate) -> tuple[date, date, int, int]:
    fact = candidate.fact
    return (
        _date_or_none(fact.get("end")) or date.min,
        _date_or_none(fact.get("filed")) or date.min,
        _int_or_none(fact.get("fy")) or 0,
        -candidate.concept_priority,
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
    if isinstance(value, Real):
        numeric_value = float(value)
        return numeric_value if math.isfinite(numeric_value) else None
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
