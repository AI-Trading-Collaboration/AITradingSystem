from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from numbers import Real
from pathlib import Path
from typing import cast

import pandas as pd

from ai_trading_system.config import (
    FundamentalFeaturesConfig,
    FundamentalRatioFeatureConfig,
    SecCompaniesConfig,
    SecCompanyConfig,
)
from ai_trading_system.fundamentals.sec_metrics import (
    SEC_FUNDAMENTAL_METRIC_COLUMNS,
    PeriodType,
    SecFundamentalMetricsCsvValidationReport,
)


class SecFundamentalFeatureIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class SecFundamentalFeatureIssue:
    severity: SecFundamentalFeatureIssueSeverity
    code: str
    message: str
    ticker: str | None = None
    feature_id: str | None = None
    period_type: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class SecFundamentalFeatureRow:
    as_of: date
    ticker: str
    period_type: PeriodType
    fiscal_year: int | None
    fiscal_period: str
    end_date: date | None
    filed_date: date | None
    feature_id: str
    feature_name: str
    value: float
    unit: str
    numerator_metric_id: str
    denominator_metric_id: str
    numerator_value: float
    denominator_value: float
    source_metric_accessions: str
    source_path: Path

    def to_record(self) -> dict[str, object]:
        return {
            "as_of": self.as_of.isoformat(),
            "ticker": self.ticker,
            "period_type": self.period_type,
            "fiscal_year": self.fiscal_year,
            "fiscal_period": self.fiscal_period,
            "end_date": self.end_date.isoformat() if self.end_date else "",
            "filed_date": self.filed_date.isoformat() if self.filed_date else "",
            "feature_id": self.feature_id,
            "feature_name": self.feature_name,
            "value": self.value,
            "unit": self.unit,
            "numerator_metric_id": self.numerator_metric_id,
            "denominator_metric_id": self.denominator_metric_id,
            "numerator_value": self.numerator_value,
            "denominator_value": self.denominator_value,
            "source_metric_accessions": self.source_metric_accessions,
            "source_path": str(self.source_path),
        }


@dataclass(frozen=True)
class SecFundamentalFeaturesReport:
    as_of: date
    input_path: Path
    validation_report: SecFundamentalMetricsCsvValidationReport
    rows: tuple[SecFundamentalFeatureRow, ...]
    issues: tuple[SecFundamentalFeatureIssue, ...] = field(default_factory=tuple)

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def company_count(self) -> int:
        return len({row.ticker for row in self.rows})

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == SecFundamentalFeatureIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == SecFundamentalFeatureIssueSeverity.WARNING
        )

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


SEC_FUNDAMENTAL_FEATURE_COLUMNS = (
    "as_of",
    "ticker",
    "period_type",
    "fiscal_year",
    "fiscal_period",
    "end_date",
    "filed_date",
    "feature_id",
    "feature_name",
    "value",
    "unit",
    "numerator_metric_id",
    "denominator_metric_id",
    "numerator_value",
    "denominator_value",
    "source_metric_accessions",
    "source_path",
)


def build_sec_fundamental_features_report(
    companies: SecCompaniesConfig,
    feature_config: FundamentalFeaturesConfig,
    input_path: Path,
    as_of: date,
    validation_report: SecFundamentalMetricsCsvValidationReport,
) -> SecFundamentalFeaturesReport:
    issues: list[SecFundamentalFeatureIssue] = []
    rows: list[SecFundamentalFeatureRow] = []

    if not validation_report.passed:
        raise ValueError("SEC 基本面指标 CSV 校验必须通过后才能构建基本面特征")

    frame = _read_sec_metrics_frame(input_path, as_of, issues)
    if frame is None:
        return SecFundamentalFeaturesReport(
            as_of=as_of,
            input_path=input_path,
            validation_report=validation_report,
            rows=(),
            issues=tuple(issues),
        )

    metric_index = _metric_index(frame)
    for company in companies.companies:
        if not company.active:
            continue
        for feature in feature_config.features:
            for period_type in _feature_periods_for_company(company, feature):
                row = _build_ratio_feature_row(
                    company=company,
                    feature=feature,
                    period_type=period_type,
                    metric_index=metric_index,
                    input_path=input_path,
                    as_of=as_of,
                    issues=issues,
                )
                if row is not None:
                    rows.append(row)

    return SecFundamentalFeaturesReport(
        as_of=as_of,
        input_path=input_path,
        validation_report=validation_report,
        rows=tuple(rows),
        issues=tuple(issues),
    )


def write_sec_fundamental_features_csv(
    report: SecFundamentalFeaturesReport,
    output_path: Path,
) -> Path:
    if not report.passed:
        raise ValueError("SEC 基本面特征报告未通过，不能写入特征 CSV")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame(
        [row.to_record() for row in report.rows],
        columns=list(SEC_FUNDAMENTAL_FEATURE_COLUMNS),
    )
    if output_path.exists():
        existing = pd.read_csv(output_path)
        if "as_of" not in existing.columns:
            raise ValueError(
                f"existing SEC fundamental features file is missing as_of: {output_path}"
            )
        existing = existing.loc[existing["as_of"] != report.as_of.isoformat()]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)

    if not new_frame.empty:
        new_frame = new_frame.sort_values(
            ["as_of", "ticker", "feature_id", "period_type"]
        ).reset_index(drop=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def render_sec_fundamental_features_report(
    report: SecFundamentalFeaturesReport,
    validation_report_path: Path,
    output_csv_path: Path,
) -> str:
    rows_by_feature = _count_by([row.feature_id for row in report.rows])
    lines = [
        "# SEC 基本面特征摘要",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入指标 CSV：`{report.input_path}`",
        f"- SEC 指标 CSV 校验状态：{report.validation_report.status}",
        f"- SEC 指标 CSV 校验报告：`{validation_report_path}`",
        f"- 特征 CSV：`{output_csv_path}`",
        f"- 公司数：{report.company_count}",
        f"- 特征行数：{report.row_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 按特征统计",
        "",
    ]

    if not rows_by_feature:
        lines.append("未生成任何 SEC 基本面特征。")
    else:
        for feature_id, count in rows_by_feature.items():
            lines.append(f"- {feature_id}：{count}")

    lines.extend(["", "## 特征明细", ""])
    if not report.rows:
        lines.append("未生成任何 SEC 基本面特征。")
    else:
        lines.extend(
            [
                "| Ticker | 特征 | 周期 | 财年 | 财期 | 截止日 | 披露日 | 数值 | 分子 | 分母 |",
                "|---|---|---|---:|---|---|---|---:|---:|---:|",
            ]
        )
        for row in sorted(
            report.rows,
            key=lambda item: (item.ticker, item.feature_id, item.period_type),
        ):
            end_date = row.end_date.isoformat() if row.end_date else ""
            filed_date = row.filed_date.isoformat() if row.filed_date else ""
            lines.append(
                "| "
                f"{row.ticker} | "
                f"{_escape_markdown_table(row.feature_name)} | "
                f"{_period_label(row.period_type)} | "
                f"{row.fiscal_year or ''} | "
                f"{row.fiscal_period} | "
                f"{end_date} | "
                f"{filed_date} | "
                f"{row.value:.4f} | "
                f"{row.numerator_value:.2f} | "
                f"{row.denominator_value:.2f} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Ticker | 特征 | 周期 | 文件 | 说明 |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.feature_id or ''} | "
                f"{issue.period_type or ''} | "
                f"{_escape_markdown_table(str(issue.path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本报告基于已通过 `validate-sec-metrics` 的 SEC 基本面指标 CSV 构建，"
            "不直接读取原始 companyfacts。",
            "- 当前特征均来自 `config/fundamental_features.yaml` 中显式声明的比率公式。",
            "- 比率特征只在分子和分母的 ticker、周期、财年、财期、截止日、"
            "披露日、单位和 accession number 对齐时生成。",
            "- 分母必须为正数；否则视为错误并阻止该特征进入下游评分。",
            "- CapEx 强度当前只按年度生成。季度 CapEx 与收入需要多期共同周期对齐，"
            "不能把不同季度的最新值直接相除。",
            "- CapEx 强度保留 SEC 指标抽取层原始符号，不在特征层取绝对值。",
            "- 已通过校验的 SEC 基本面特征会进入 `score-daily` 的基本面硬数据评分。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_sec_fundamental_features_report(
    report: SecFundamentalFeaturesReport,
    validation_report_path: Path,
    output_csv_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_sec_fundamental_features_report(
            report,
            validation_report_path=validation_report_path,
            output_csv_path=output_csv_path,
        ),
        encoding="utf-8",
    )
    return output_path


def default_sec_fundamental_features_csv_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"sec_fundamental_features_{as_of.isoformat()}.csv"


def default_sec_fundamental_features_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"sec_fundamental_features_{as_of.isoformat()}.md"


def _read_sec_metrics_frame(
    input_path: Path,
    as_of: date,
    issues: list[SecFundamentalFeatureIssue],
) -> pd.DataFrame | None:
    try:
        frame = pd.read_csv(input_path)
    except Exception as exc:
        issues.append(
            SecFundamentalFeatureIssue(
                severity=SecFundamentalFeatureIssueSeverity.ERROR,
                code="sec_fundamental_features_input_unreadable",
                path=input_path,
                message=f"SEC 基本面指标 CSV 无法读取：{exc}",
            )
        )
        return None

    missing_columns = sorted(set(SEC_FUNDAMENTAL_METRIC_COLUMNS) - set(frame.columns))
    if missing_columns:
        issues.append(
            SecFundamentalFeatureIssue(
                severity=SecFundamentalFeatureIssueSeverity.ERROR,
                code="sec_fundamental_features_missing_input_columns",
                path=input_path,
                message=f"SEC 基本面指标 CSV 缺少字段：{', '.join(missing_columns)}。",
            )
        )
        return None

    return frame.loc[frame["as_of"].astype(str) == as_of.isoformat()].copy()


def _metric_index(frame: pd.DataFrame) -> dict[tuple[str, str, str], dict[str, object]]:
    records = cast(list[dict[str, object]], frame.to_dict(orient="records"))
    return {
        (
            str(record.get("ticker", "")).upper(),
            str(record.get("period_type", "")),
            str(record.get("metric_id", "")),
        ): record
        for record in records
    }


def _feature_periods_for_company(
    company: SecCompanyConfig,
    feature: FundamentalRatioFeatureConfig,
) -> list[PeriodType]:
    supported_periods = set(company.sec_metric_periods)
    return [
        period_type
        for period_type in feature.preferred_periods
        if period_type in supported_periods
    ]


def _build_ratio_feature_row(
    company: SecCompanyConfig,
    feature: FundamentalRatioFeatureConfig,
    period_type: PeriodType,
    metric_index: dict[tuple[str, str, str], dict[str, object]],
    input_path: Path,
    as_of: date,
    issues: list[SecFundamentalFeatureIssue],
) -> SecFundamentalFeatureRow | None:
    numerator = metric_index.get(
        (company.ticker, period_type, feature.numerator_metric_id)
    )
    denominator = metric_index.get(
        (company.ticker, period_type, feature.denominator_metric_id)
    )
    if numerator is None or denominator is None:
        missing_metric = (
            feature.numerator_metric_id
            if numerator is None
            else feature.denominator_metric_id
        )
        issues.append(
            SecFundamentalFeatureIssue(
                severity=SecFundamentalFeatureIssueSeverity.WARNING,
                code="sec_fundamental_feature_missing_metric",
                ticker=company.ticker,
                feature_id=feature.feature_id,
                period_type=period_type,
                path=input_path,
                message=f"缺少构建该特征所需指标：{missing_metric}。",
            )
        )
        return None

    if not _records_can_form_ratio(numerator, denominator):
        issues.append(
            SecFundamentalFeatureIssue(
                severity=SecFundamentalFeatureIssueSeverity.ERROR,
                code="sec_fundamental_feature_metric_alignment_mismatch",
                ticker=company.ticker,
                feature_id=feature.feature_id,
                period_type=period_type,
                path=input_path,
                message="分子和分母指标的周期、单位或披露来源不一致，已停止生成该特征。",
            )
        )
        return None

    numerator_value = _numeric_value(numerator.get("value"))
    denominator_value = _numeric_value(denominator.get("value"))
    if numerator_value is None or denominator_value is None:
        issues.append(
            SecFundamentalFeatureIssue(
                severity=SecFundamentalFeatureIssueSeverity.ERROR,
                code="sec_fundamental_feature_metric_value_invalid",
                ticker=company.ticker,
                feature_id=feature.feature_id,
                period_type=period_type,
                path=input_path,
                message="分子或分母指标不是有效数值。",
            )
        )
        return None
    if denominator_value <= 0:
        issues.append(
            SecFundamentalFeatureIssue(
                severity=SecFundamentalFeatureIssueSeverity.ERROR,
                code="sec_fundamental_feature_denominator_non_positive",
                ticker=company.ticker,
                feature_id=feature.feature_id,
                period_type=period_type,
                path=input_path,
                message="分母指标必须为正数，无法构建稳定比率特征。",
            )
        )
        return None

    return SecFundamentalFeatureRow(
        as_of=as_of,
        ticker=company.ticker,
        period_type=period_type,
        fiscal_year=_int_or_none(numerator.get("fiscal_year")),
        fiscal_period=str(numerator.get("fiscal_period") or ""),
        end_date=_date_or_none(numerator.get("end_date")),
        filed_date=_date_or_none(numerator.get("filed_date")),
        feature_id=feature.feature_id,
        feature_name=feature.name,
        value=numerator_value / denominator_value,
        unit=feature.unit,
        numerator_metric_id=feature.numerator_metric_id,
        denominator_metric_id=feature.denominator_metric_id,
        numerator_value=numerator_value,
        denominator_value=denominator_value,
        source_metric_accessions=str(numerator.get("accession_number") or ""),
        source_path=input_path,
    )


def _records_can_form_ratio(
    numerator: dict[str, object],
    denominator: dict[str, object],
) -> bool:
    alignment_columns = (
        "ticker",
        "period_type",
        "fiscal_year",
        "fiscal_period",
        "end_date",
        "filed_date",
        "unit",
        "accession_number",
    )
    return all(
        str(numerator.get(column, "")) == str(denominator.get(column, ""))
        for column in alignment_columns
    )


def _numeric_value(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Real):
        numeric_value = float(value)
        return numeric_value if math.isfinite(numeric_value) else None
    return None


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, Real):
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            return None
        return int(numeric_value)
    if isinstance(value, str) and value:
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _date_or_none(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _count_by(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _period_label(period_type: str) -> str:
    return {"annual": "年度", "quarterly": "季度"}.get(period_type, period_type)


def _severity_label(severity: SecFundamentalFeatureIssueSeverity) -> str:
    if severity == SecFundamentalFeatureIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
