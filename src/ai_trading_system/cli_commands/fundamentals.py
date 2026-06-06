from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ai_trading_system.config import (
    DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    PROJECT_ROOT,
    load_fundamental_features,
    load_fundamental_metrics,
    load_sec_companies,
)
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecEdgarCompanyFactsProvider,
    download_sec_companyfacts,
)
from ai_trading_system.fundamentals.sec_features import (
    build_sec_fundamental_features_report,
    default_sec_fundamental_features_csv_path,
    default_sec_fundamental_features_report_path,
    write_sec_fundamental_features_csv,
    write_sec_fundamental_features_report,
)
from ai_trading_system.fundamentals.sec_filings import (
    DEFAULT_SEC_FILING_ARCHIVE_DIR,
    DEFAULT_SEC_SUBMISSIONS_DIR,
    SecEdgarFilingArchiveProvider,
    build_sec_accession_coverage_report,
    default_sec_accession_coverage_report_path,
    download_sec_filing_archive_indexes,
    download_sec_submissions,
    write_sec_accession_coverage_report,
)
from ai_trading_system.fundamentals.sec_metrics import (
    build_sec_fundamental_metrics_report,
    default_sec_fundamental_metrics_csv_path,
    default_sec_fundamental_metrics_report_path,
    default_sec_fundamental_metrics_validation_report_path,
    load_sec_fundamental_metric_rows_csv,
    validate_sec_fundamental_metric_rows,
    validate_sec_fundamental_metrics_csv,
    write_sec_fundamental_metric_rows_csv,
    write_sec_fundamental_metrics_csv,
    write_sec_fundamental_metrics_report,
    write_sec_fundamental_metrics_validation_report,
)
from ai_trading_system.fundamentals.sec_validation import (
    default_sec_companyfacts_validation_report_path,
    validate_sec_companyfacts_cache,
    write_sec_companyfacts_validation_report,
)
from ai_trading_system.fundamentals.tsm_ir import (
    TsmIrHttpProvider,
    build_tsm_ir_quarterly_batch_import_report,
    build_tsm_ir_sec_metric_conversion_report,
    extract_tsm_ir_pdf_text,
    load_tsm_ir_quarterly_metric_rows_csv,
    merge_tsm_ir_quarterly_rows_into_sec_metrics,
    parse_tsm_ir_management_report_text,
    select_tsm_ir_management_report_resource,
    select_tsm_ir_quarterly_metric_rows_as_of,
    write_tsm_ir_pdf_text_extraction_report,
    write_tsm_ir_quarterly_batch_import_report,
    write_tsm_ir_quarterly_batch_metrics_csv,
    write_tsm_ir_quarterly_metrics_csv,
    write_tsm_ir_quarterly_report,
)

console = Console()
fundamentals_app = typer.Typer(help="基本面数据源下载和审计。", no_args_is_help=True)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if not normalized:
        raise typer.BadParameter("时间不能为空。")
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        if "T" not in normalized and " " not in normalized:
            parsed_date = date.fromisoformat(normalized)
            return datetime.combine(parsed_date, time.min, tzinfo=UTC)
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise typer.BadParameter("时间必须使用 ISO datetime 或 YYYY-MM-DD 格式。") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@fundamentals_app.command("list-sec-companies")
def list_sec_companies(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃公司，或显示全部公司。"),
    ] = True,
) -> None:
    """列出 SEC companyfacts 下载配置。"""
    config = load_sec_companies(config_path)

    table = Table(title="SEC Company Facts 公司映射")
    table.add_column("Ticker")
    table.add_column("CIK")
    table.add_column("公司")
    table.add_column("活跃")
    table.add_column("Taxonomy")
    table.add_column("SEC 指标周期")
    for company in sorted(config.companies, key=lambda item: item.ticker):
        if active_only and not company.active:
            continue
        table.add_row(
            company.ticker,
            company.cik,
            company.company_name,
            "是" if company.active else "否",
            ", ".join(company.expected_taxonomies),
            ", ".join(company.sec_metric_periods),
        )
    console.print(table)


@fundamentals_app.command("download-sec-companyfacts")
def download_sec_companyfacts_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输出目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "sec_companyfacts",
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔的 ticker；未提供时下载全部活跃配置。"),
    ] = None,
    user_agent: Annotated[
        str | None,
        typer.Option(
            "--user-agent",
            envvar="SEC_USER_AGENT",
            help="SEC fair access 要求的 User-Agent；也可使用 SEC_USER_AGENT 环境变量。",
        ),
    ] = None,
) -> None:
    """下载 SEC companyfacts 原始 JSON，并写入审计 manifest。"""
    if user_agent is None or not user_agent.strip():
        raise typer.BadParameter(
            "SEC companyfacts 下载必须提供 --user-agent 或 SEC_USER_AGENT；"
            "格式建议包含项目/组织名称和联系邮箱。"
        )

    selected_tickers = _parse_csv_items(tickers) if tickers else None
    summary = download_sec_companyfacts(
        config=load_sec_companies(config_path),
        output_dir=output_dir,
        provider=SecEdgarCompanyFactsProvider(user_agent=user_agent),
        tickers=selected_tickers,
    )

    console.print("[green]SEC companyfacts 缓存已更新。[/green]")
    console.print(f"公司数量：{summary.company_count}")
    console.print(f"事实数量：{summary.total_fact_count}")
    console.print(f"输出目录：{summary.output_dir}")
    console.print(f"下载审计清单：{summary.manifest_path}")


@fundamentals_app.command("download-sec-submissions")
def download_sec_submissions_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="SEC submissions 原始 JSON 输出目录。"),
    ] = DEFAULT_SEC_SUBMISSIONS_DIR,
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔的 ticker；未提供时下载全部活跃配置。"),
    ] = None,
    user_agent: Annotated[
        str | None,
        typer.Option(
            "--user-agent",
            envvar="SEC_USER_AGENT",
            help="SEC fair access 要求的 User-Agent；也可使用 SEC_USER_AGENT 环境变量。",
        ),
    ] = None,
) -> None:
    """下载 SEC submissions filing history，并写入审计 manifest。"""
    if user_agent is None or not user_agent.strip():
        raise typer.BadParameter(
            "SEC submissions 下载必须提供 --user-agent 或 SEC_USER_AGENT；"
            "格式建议包含项目/组织名称和联系邮箱。"
        )

    selected_tickers = _parse_csv_items(tickers) if tickers else None
    summary = download_sec_submissions(
        config=load_sec_companies(config_path),
        output_dir=output_dir,
        provider=SecEdgarFilingArchiveProvider(user_agent=user_agent),
        tickers=selected_tickers,
    )

    console.print("[green]SEC submissions 缓存已更新。[/green]")
    console.print(f"公司数量：{summary.company_count}")
    console.print(f"Filing 数量：{summary.filing_count}")
    console.print(f"输出目录：{summary.output_dir}")
    console.print(f"下载审计清单：{summary.manifest_path}")


@fundamentals_app.command("download-sec-filing-archive")
def download_sec_filing_archive_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    metrics_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="SEC accession directory index.json 输出目录。"),
    ] = DEFAULT_SEC_FILING_ARCHIVE_DIR,
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔的 ticker；未提供时读取当日全部 accession。"),
    ] = None,
    request_delay_seconds: Annotated[
        float,
        typer.Option(help="每个 SEC archive 请求之间的等待秒数。"),
    ] = 0.2,
    user_agent: Annotated[
        str | None,
        typer.Option(
            "--user-agent",
            envvar="SEC_USER_AGENT",
            help="SEC fair access 要求的 User-Agent；也可使用 SEC_USER_AGENT 环境变量。",
        ),
    ] = None,
) -> None:
    """按 SEC 指标 CSV 已使用 accession 下载 archive index.json。"""
    if user_agent is None or not user_agent.strip():
        raise typer.BadParameter(
            "SEC filing archive 下载必须提供 --user-agent 或 SEC_USER_AGENT；"
            "格式建议包含项目/组织名称和联系邮箱。"
        )

    archive_date = _parse_date(as_of) if as_of else date.today()
    metrics_input = metrics_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        archive_date,
    )
    selected_tickers = _parse_csv_items(tickers) if tickers else None
    summary = download_sec_filing_archive_indexes(
        metrics_path=metrics_input,
        as_of=archive_date,
        output_dir=output_dir,
        provider=SecEdgarFilingArchiveProvider(user_agent=user_agent),
        tickers=selected_tickers,
        request_delay_seconds=request_delay_seconds,
    )

    console.print("[green]SEC filing archive index 已更新。[/green]")
    console.print(f"Accession 数量：{summary.accession_count}")
    console.print(f"输出目录：{summary.output_dir}")
    console.print(f"下载审计清单：{summary.manifest_path}")


@fundamentals_app.command("sec-accession-coverage")
def sec_accession_coverage_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    metrics_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    submissions_dir: Annotated[
        Path,
        typer.Option(help="SEC submissions 原始 JSON 输入目录。"),
    ] = DEFAULT_SEC_SUBMISSIONS_DIR,
    filing_archive_dir: Annotated[
        Path,
        typer.Option(help="SEC accession directory index.json 输入目录。"),
    ] = DEFAULT_SEC_FILING_ARCHIVE_DIR,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC accession archive 覆盖报告输出路径。"),
    ] = None,
) -> None:
    """检查 SEC 指标 CSV 已用 accession 的 submissions/archive 覆盖。"""
    coverage_date = _parse_date(as_of) if as_of else date.today()
    metrics_input = metrics_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        coverage_date,
    )
    report_output = output_path or default_sec_accession_coverage_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        coverage_date,
    )
    report = build_sec_accession_coverage_report(
        metrics_path=metrics_input,
        submissions_dir=submissions_dir,
        filing_archive_dir=filing_archive_dir,
        as_of=coverage_date,
    )
    write_sec_accession_coverage_report(report, report_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC accession 覆盖状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"Accession 数：{report.accession_count}；已覆盖：{report.covered_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("validate-sec-companyfacts")
def validate_sec_companyfacts_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    input_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输入目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "sec_companyfacts",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 SEC companyfacts 原始缓存、CIK、taxonomy 和 manifest。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_sec_companyfacts_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_sec_companyfacts_cache(
        config=load_sec_companies(config_path),
        input_dir=input_dir,
        as_of=validation_date,
    )
    write_sec_companyfacts_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC companyfacts 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"配置公司数：{report.file_count}；已缓存：{report.available_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("extract-sec-metrics")
def extract_sec_metrics_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    input_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输入目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "sec_companyfacts",
    as_of: Annotated[
        str | None,
        typer.Option(help="抽取日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 校验报告输出路径。"),
    ] = None,
) -> None:
    """在 SEC 缓存校验通过后抽取结构化基本面指标。"""
    extraction_date = _parse_date(as_of) if as_of else date.today()
    validation_output = validation_report_path or default_sec_companyfacts_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        extraction_date,
    )
    csv_output = output_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        extraction_date,
    )
    markdown_output = report_path or default_sec_fundamental_metrics_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        extraction_date,
    )

    sec_companies = load_sec_companies(sec_companies_path)
    validation_report = validate_sec_companyfacts_cache(
        config=sec_companies,
        input_dir=input_dir,
        as_of=extraction_date,
    )
    write_sec_companyfacts_validation_report(validation_report, validation_output)
    if not validation_report.passed:
        console.print("[red]SEC companyfacts 质量门禁失败，已停止指标抽取。[/red]")
        console.print(f"SEC 缓存校验报告：{validation_output}")
        console.print(
            f"错误数：{validation_report.error_count}；"
            f"警告数：{validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    report = build_sec_fundamental_metrics_report(
        companies=sec_companies,
        metrics=load_fundamental_metrics(metrics_path),
        input_dir=input_dir,
        as_of=extraction_date,
        validation_report=validation_report,
    )
    csv_path = write_sec_fundamental_metrics_csv(report, csv_output)
    markdown_path = write_sec_fundamental_metrics_report(
        report=report,
        validation_report_path=validation_output,
        output_csv_path=csv_path,
        output_path=markdown_output,
    )

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC 基本面指标抽取状态：{report.status}[/{status_style}]")
    console.print(f"SEC 缓存校验报告：{validation_output}（{validation_report.status}）")
    console.print(f"指标 CSV：{csv_path}")
    console.print(f"指标报告：{markdown_path}")
    console.print(f"公司数：{report.company_count}；指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("validate-sec-metrics")
def validate_sec_metrics_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    input_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验已抽取的 SEC 基本面指标 CSV。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    csv_input = input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        validation_date,
    )
    report_output = output_path or default_sec_fundamental_metrics_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_sec_fundamental_metrics_csv(
        companies=load_sec_companies(sec_companies_path),
        metrics=load_fundamental_metrics(metrics_path),
        input_path=csv_input,
        as_of=validation_date,
    )
    write_sec_fundamental_metrics_validation_report(report, report_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC 基本面指标 CSV 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"输入文件：{csv_input}")
    console.print(
        f"覆盖率：{report.coverage:.0%}；"
        f"当日行数：{report.as_of_row_count}；"
        f"总行数：{report.row_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("build-sec-features")
def build_sec_features_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于先校验指标 CSV。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    feature_config_path: Annotated[
        Path,
        typer.Option(help="SEC 基本面特征公式配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    input_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="特征日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面特征 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面特征报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """在 SEC 指标 CSV 校验通过后构建基本面比率特征。"""
    feature_date = _parse_date(as_of) if as_of else date.today()
    csv_input = input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        feature_date,
    )
    validation_output = (
        validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            feature_date,
        )
    )
    feature_csv_output = output_path or default_sec_fundamental_features_csv_path(
        PROJECT_ROOT / "data" / "processed",
        feature_date,
    )
    feature_report_output = report_path or default_sec_fundamental_features_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )

    sec_companies = load_sec_companies(sec_companies_path)
    metrics = load_fundamental_metrics(metrics_path)
    validation_report = validate_sec_fundamental_metrics_csv(
        companies=sec_companies,
        metrics=metrics,
        input_path=csv_input,
        as_of=feature_date,
    )
    write_sec_fundamental_metrics_validation_report(validation_report, validation_output)
    if not validation_report.passed:
        console.print("[red]SEC 基本面指标 CSV 校验失败，已停止特征构建。[/red]")
        console.print(f"SEC 指标 CSV 校验报告：{validation_output}")
        console.print(
            f"错误数：{validation_report.error_count}；"
            f"警告数：{validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    report = build_sec_fundamental_features_report(
        companies=sec_companies,
        feature_config=load_fundamental_features(feature_config_path),
        input_path=csv_input,
        as_of=feature_date,
        validation_report=validation_report,
    )
    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    if not report.passed:
        markdown_path = write_sec_fundamental_features_report(
            report=report,
            validation_report_path=validation_output,
            output_csv_path=feature_csv_output,
            output_path=feature_report_output,
        )
        console.print(f"[{status_style}]SEC 基本面特征构建状态：{report.status}[/{status_style}]")
        console.print(f"SEC 指标 CSV 校验报告：{validation_output}（{validation_report.status}）")
        console.print(f"基本面特征 CSV 未写入：{feature_csv_output}")
        console.print(f"基本面特征报告：{markdown_path}")
        console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
        raise typer.Exit(code=1)

    csv_path = write_sec_fundamental_features_csv(report, feature_csv_output)
    markdown_path = write_sec_fundamental_features_report(
        report=report,
        validation_report_path=validation_output,
        output_csv_path=csv_path,
        output_path=feature_report_output,
    )

    console.print(f"[{status_style}]SEC 基本面特征构建状态：{report.status}[/{status_style}]")
    console.print(f"SEC 指标 CSV 校验报告：{validation_output}（{validation_report.status}）")
    console.print(f"基本面特征 CSV：{csv_path}")
    console.print(f"基本面特征报告：{markdown_path}")
    console.print(f"公司数：{report.company_count}；特征行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("extract-tsm-ir-pdf-text")
def extract_tsm_ir_pdf_text_command(
    input_path: Annotated[
        Path,
        typer.Option(help="本地 TSMC IR Management Report 官方 PDF 输入路径。"),
    ],
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方 PDF URL。"),
    ],
    output_path: Annotated[
        Path | None,
        typer.Option(help="抽取后的 Management Report 文本输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    extracted_at: Annotated[
        str | None,
        typer.Option(help="抽取时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR PDF 文本抽取报告输出路径。"),
    ] = None,
) -> None:
    """从 TSMC IR 官方 PDF 的可抽取文本层生成 Management Report 文本。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    extraction_datetime = _parse_datetime(extracted_at) if extracted_at else datetime.now(tz=UTC)
    text_output = output_path or (
        PROJECT_ROOT / "data" / "external" / "fundamentals" / "tsm_ir" / f"{input_path.stem}.txt"
    )
    markdown_output = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"tsm_ir_pdf_text_{report_date}.md"
    )

    report = extract_tsm_ir_pdf_text(
        input_path=input_path,
        source_url=source_url,
        output_path=text_output,
        extracted_at=extraction_datetime,
    )
    markdown_path = write_tsm_ir_pdf_text_extraction_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR PDF 文本抽取状态：{report.status}[/{status_style}]")
    console.print(f"Source URL：{source_url}")
    console.print(f"PDF：{input_path}")
    console.print(f"抽取文本：{text_output}")
    console.print(f"报告：{markdown_path}")
    console.print(f"页数：{report.page_count}；字符数：{report.character_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR PDF 文本抽取未通过，未生成可用于导入的可信文本。[/red]")
        raise typer.Exit(code=1)


@fundamentals_app.command("import-tsm-ir-quarterly")
def import_tsm_ir_quarterly(
    input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR Management Report 已抽取文本输入路径。"),
    ],
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方季度页面或 Management Report URL。"),
    ],
    fiscal_year: Annotated[
        int,
        typer.Option(help="财年，例如 2026。"),
    ],
    fiscal_period: Annotated[
        str,
        typer.Option(help="财期，例如 Q1、Q2、Q3 或 Q4。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    filed_date: Annotated[
        str | None,
        typer.Option(
            help=(
                "TSMC IR Management Report 公开/披露日期，格式为 YYYY-MM-DD；"
                "未提供时使用 captured_at 日期。"
            ),
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 季度基本面报告输出路径。"),
    ] = None,
) -> None:
    """从 TSMC IR 官方 Management Report 文本导入季度基本面指标。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    disclosed_date = _parse_date(filed_date) if filed_date else None
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    normalized_period = fiscal_period.upper()
    if normalized_period not in {"Q1", "Q2", "Q3", "Q4"}:
        raise typer.BadParameter("财期必须是 Q1、Q2、Q3 或 Q4。")
    markdown_output = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"tsm_ir_quarterly_{fiscal_year}_{normalized_period}_{import_date}.md"
    )
    report = parse_tsm_ir_management_report_text(
        text=input_path.read_text(encoding="utf-8"),
        source_url=source_url,
        fiscal_year=fiscal_year,
        fiscal_period=normalized_period,
        as_of=import_date,
        captured_at=captured_datetime,
        source_path=input_path,
        filed_date=disclosed_date,
    )
    markdown_path = write_tsm_ir_quarterly_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR 季度基本面导入状态：{report.status}[/{status_style}]")
    console.print(f"报告：{markdown_path}")
    console.print(f"指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("import-tsm-ir-quarterly-batch")
def import_tsm_ir_quarterly_batch(
    manifest_path: Annotated[
        Path,
        typer.Option(
            help=(
                "TSMC IR 批量导入 manifest CSV，字段为 fiscal_year,"
                "fiscal_period,source_url,input_path。"
            ),
        ),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 批量季度基本面报告输出路径。"),
    ] = None,
) -> None:
    """按 manifest 批量导入多个 TSMC IR Management Report 文本。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    markdown_output = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"tsm_ir_quarterly_batch_{import_date}.md"
    )

    report = build_tsm_ir_quarterly_batch_import_report(
        manifest_path=manifest_path,
        as_of=import_date,
        captured_at=captured_datetime,
        output_path=csv_output,
    )
    markdown_path = write_tsm_ir_quarterly_batch_import_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]TSMC IR 批量季度基本面导入状态：" f"{report.status}[/{status_style}]"
    )
    console.print(f"Manifest：{manifest_path}")
    console.print(f"报告：{markdown_path}")
    console.print(f"季度条目数：{report.entry_count}；指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 批量季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_batch_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("fetch-tsm-ir-quarterly")
def fetch_tsm_ir_quarterly(
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方季度页面 URL。"),
    ],
    fiscal_year: Annotated[
        int,
        typer.Option(help="财年，例如 2026。"),
    ],
    fiscal_period: Annotated[
        str,
        typer.Option(help="财期，例如 Q1、Q2、Q3 或 Q4。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    filed_date: Annotated[
        str | None,
        typer.Option(
            help=(
                "TSMC IR Management Report 公开/披露日期，格式为 YYYY-MM-DD；"
                "未提供时使用 captured_at 日期。"
            ),
        ),
    ] = None,
    source_text_path: Annotated[
        Path | None,
        typer.Option(help="保存下载到的 Management Report 文本路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 季度基本面报告输出路径。"),
    ] = None,
    timeout: Annotated[
        float,
        typer.Option(help="TSMC IR HTTP 请求超时时间，单位秒。"),
    ] = 30.0,
    user_agent: Annotated[
        str,
        typer.Option(
            "--user-agent",
            envvar="TSM_IR_USER_AGENT",
            help="TSMC IR HTTP User-Agent；也可使用 TSM_IR_USER_AGENT 环境变量。",
        ),
    ] = "ai-trading-system tsm-ir/0.1",
) -> None:
    """从 TSMC IR 官方季度页面发现并下载 Management Report 文本后导入指标。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    disclosed_date = _parse_date(filed_date) if filed_date else None
    normalized_period = fiscal_period.upper()
    if normalized_period not in {"Q1", "Q2", "Q3", "Q4"}:
        raise typer.BadParameter("财期必须是 Q1、Q2、Q3 或 Q4。")

    text_output = source_text_path or (
        PROJECT_ROOT
        / "data"
        / "external"
        / "fundamentals"
        / "tsm_ir"
        / f"{fiscal_year}_{normalized_period}_management_report_{import_date}.txt"
    )
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    markdown_output = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"tsm_ir_quarterly_{fiscal_year}_{normalized_period}_{import_date}.md"
    )

    provider = TsmIrHttpProvider(timeout=timeout, user_agent=user_agent)
    try:
        management_resource = select_tsm_ir_management_report_resource(
            provider=provider,
            source_url=source_url,
        )
        management_text = provider.download_text(management_resource.url)
    except Exception as exc:
        console.print("[red]TSMC IR 官方页面或 Management Report 文本下载失败。[/red]")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    text_output.parent.mkdir(parents=True, exist_ok=True)
    text_output.write_text(management_text, encoding="utf-8")
    report = parse_tsm_ir_management_report_text(
        text=management_text,
        source_url=management_resource.url,
        fiscal_year=fiscal_year,
        fiscal_period=normalized_period,
        as_of=import_date,
        captured_at=captured_datetime,
        source_path=text_output,
        filed_date=disclosed_date,
    )
    markdown_path = write_tsm_ir_quarterly_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR 季度基本面抓取状态：{report.status}[/{status_style}]")
    console.print(f"季度页面：{source_url}")
    console.print(f"Management Report URL：{management_resource.url}")
    console.print(f"原始文本：{text_output}")
    console.print(f"报告：{markdown_path}")
    console.print(f"指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("merge-tsm-ir-sec-metrics")
def merge_tsm_ir_sec_metrics(
    input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR 季度指标 CSV 输入路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "tsm_ir_quarterly_metrics.csv",
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径；TSM 必须声明 quarterly。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于合并后校验。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    sec_input_path: Annotated[
        Path | None,
        typer.Option(help="既有 SEC 基本面指标 CSV；未提供时使用 as_of 默认路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="合并评估日期，格式为 YYYY-MM-DD；默认使用 TSM IR 输入最新 as_of。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="合并后的 SEC-style 基本面指标 CSV 输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """把 TSMC IR 季度指标合并到统一 SEC-style 基本面指标 CSV。"""
    if not input_path.exists():
        raise typer.BadParameter(f"TSMC IR 季度指标 CSV 不存在：{input_path}")

    all_tsm_rows = load_tsm_ir_quarterly_metric_rows_csv(input_path)
    if not all_tsm_rows:
        raise typer.BadParameter("TSMC IR 季度指标 CSV 没有可合并的指标行。")
    merge_date = _parse_date(as_of) if as_of else max(row.as_of for row in all_tsm_rows)
    tsm_rows = tuple(
        replace(row, as_of=merge_date)
        for row in select_tsm_ir_quarterly_metric_rows_as_of(
            all_tsm_rows,
            merge_date,
        )
    )
    if not tsm_rows:
        raise typer.BadParameter(
            f"TSMC IR 季度指标 CSV 不包含 {merge_date.isoformat()} 可用的已披露季度记录。"
        )

    sec_input = sec_input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        merge_date,
    )
    csv_output = output_path or sec_input
    validation_output = (
        validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            merge_date,
        )
    )
    sec_companies = load_sec_companies(sec_companies_path)
    conversion_report = build_tsm_ir_sec_metric_conversion_report(tsm_rows, sec_companies)
    if not conversion_report.passed:
        console.print("[red]TSMC IR 转换为 SEC-style 指标失败，已停止合并。[/red]")
        console.print(
            f"错误数：{conversion_report.error_count}；"
            f"警告数：{conversion_report.warning_count}"
        )
        raise typer.Exit(code=1)

    existing_rows = load_sec_fundamental_metric_rows_csv(sec_input)
    merged_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics(
        existing_rows=existing_rows,
        tsm_rows=tsm_rows,
        tsm_company=sec_companies,
    )
    csv_path = write_sec_fundamental_metric_rows_csv(merged_rows, csv_output)
    validation_report = validate_sec_fundamental_metric_rows(
        companies=sec_companies,
        metrics=load_fundamental_metrics(metrics_path),
        rows=merged_rows,
        source_path=csv_path,
        as_of=merge_date,
    )
    write_sec_fundamental_metrics_validation_report(validation_report, validation_output)

    status_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(
        f"[{status_style}]TSMC IR 合并后 SEC 指标校验状态："
        f"{validation_report.status}[/{status_style}]"
    )
    console.print(f"合并日期：{merge_date.isoformat()}")
    console.print(f"TSMC IR 转换行数：{len(conversion_report.rows)}")
    console.print(f"既有 SEC 指标行数：{len(existing_rows)}；合并后行数：{len(merged_rows)}")
    console.print(f"输出 CSV：{csv_path}")
    console.print(f"校验报告：{validation_output}")
    console.print(
        f"错误数：{validation_report.error_count}；警告数：{validation_report.warning_count}"
    )
    if not validation_report.passed:
        raise typer.Exit(code=1)
