from __future__ import annotations

import os
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import (
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    PROJECT_ROOT,
    load_data_sources,
    load_universe,
)
from ai_trading_system.fmp_forward_pit import (
    DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
    DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
    FmpForwardPitFetchReport,
    FmpForwardPitIssue,
    FmpForwardPitIssueSeverity,
    attach_fmp_forward_pit_raw_paths,
    attach_fmp_forward_pit_report_artifacts,
    build_fmp_forward_pit_failure_report,
    default_fmp_forward_pit_fetch_report_path,
    default_fmp_forward_pit_normalized_path,
    fetch_fmp_forward_pit_snapshots,
    sanitize_fmp_forward_pit_error_message,
    write_fmp_forward_pit_fetch_report,
    write_fmp_forward_pit_normalized_csv_from_payloads,
    write_fmp_forward_pit_raw_payloads,
)
from ai_trading_system.pit_snapshots import (
    DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    default_pit_snapshot_validation_report_path,
    discover_existing_pit_raw_snapshots,
    validate_pit_snapshot_manifest,
    write_pit_snapshot_manifest,
    write_pit_snapshot_validation_report,
)
from ai_trading_system.valuation_sources import (
    default_eodhd_earnings_trends_raw_dir,
    default_fmp_analyst_estimate_history_dir,
    default_fmp_historical_valuation_raw_dir,
)

console = Console()
pit_snapshots_app = typer.Typer(help="Forward-only PIT raw snapshot 归档。", no_args_is_help=True)

DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR = default_fmp_analyst_estimate_history_dir(
    PROJECT_ROOT / "data" / "raw"
)
DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR = default_fmp_historical_valuation_raw_dir(
    PROJECT_ROOT / "data" / "raw"
)
DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR = default_eodhd_earnings_trends_raw_dir(
    PROJECT_ROOT / "data" / "raw"
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD 格式。") from exc


def _parse_csv_items(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@pit_snapshots_app.command("validate")
def validate_pit_snapshots_command(
    input_path: Annotated[
        Path,
        typer.Option(help="PIT raw snapshot manifest CSV 路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录 YAML 路径，用于校验授权和 provider 信息。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 快照质量报告输出路径。"),
    ] = None,
) -> None:
    """校验 forward-only PIT raw snapshot manifest。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_pit_snapshot_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_pit_snapshot_manifest(
        input_path=input_path,
        as_of=validation_date,
        data_sources=load_data_sources(data_sources_path),
    )
    write_pit_snapshot_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]PIT 快照归档状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Manifest：{input_path}")
    console.print(f"快照数：{report.snapshot_count}；原始记录数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@pit_snapshots_app.command("build-manifest")
def build_pit_snapshot_manifest_command(
    output_path: Annotated[
        Path,
        typer.Option(help="生成的 PIT raw snapshot manifest CSV 路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录 YAML 路径，用于补充授权字段。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    fmp_analyst_history_dir: Annotated[
        Path,
        typer.Option(help="FMP analyst estimates 原始历史快照目录。"),
    ] = DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
    fmp_historical_valuation_dir: Annotated[
        Path,
        typer.Option(help="FMP historical valuation 原始 payload 目录。"),
    ] = DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR,
    eodhd_earnings_trends_dir: Annotated[
        Path,
        typer.Option(help="EODHD Earnings Trends 原始 payload 目录。"),
    ] = DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR,
    fmp_forward_pit_dir: Annotated[
        Path,
        typer.Option(help="FMP forward-only PIT 原始 payload 目录。"),
    ] = DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 快照质量报告输出路径。"),
    ] = None,
) -> None:
    """从现有 FMP/EODHD raw cache 生成通用 PIT raw snapshot manifest。"""
    manifest_date = _parse_date(as_of) if as_of else date.today()
    data_sources = load_data_sources(data_sources_path)
    records = discover_existing_pit_raw_snapshots(
        fmp_analyst_history_dir=fmp_analyst_history_dir,
        fmp_historical_valuation_dir=fmp_historical_valuation_dir,
        eodhd_earnings_trends_dir=eodhd_earnings_trends_dir,
        fmp_forward_pit_dir=fmp_forward_pit_dir,
        data_sources=data_sources,
    )
    manifest_path = write_pit_snapshot_manifest(records, output_path)
    report_path = validation_report_path or default_pit_snapshot_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        manifest_date,
    )
    report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=manifest_date,
        data_sources=data_sources,
    )
    write_pit_snapshot_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"生成 PIT manifest：{manifest_path}")
    console.print(f"[{status_style}]PIT 快照归档状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"快照数：{report.snapshot_count}；原始记录数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@pit_snapshots_app.command("fetch-fmp-forward")
def fetch_fmp_forward_pit_command(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    raw_output_dir: Annotated[
        Path,
        typer.Option(help="写入 FMP forward-only PIT 原始 JSON 的目录。"),
    ] = DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
    normalized_output_path: Annotated[
        Path | None,
        typer.Option(help="写入 FMP forward-only PIT 标准化 CSV 的路径。"),
    ] = None,
    manifest_path: Annotated[
        Path,
        typer.Option(help="写入或刷新 PIT raw snapshot manifest CSV 的路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="抓取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP forward PIT 抓取报告输出路径。"),
    ] = None,
    pit_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 快照质量报告输出路径。"),
    ] = None,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录 YAML 路径，用于补充 PIT manifest 授权字段。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    analyst_estimate_limit: Annotated[
        int,
        typer.Option(help="每个 ticker 拉取的 annual analyst estimate 记录数。"),
    ] = 10,
    earnings_calendar_lookback_days: Annotated[
        int,
        typer.Option(help="earnings-calendar 向前覆盖天数。"),
    ] = 7,
    earnings_calendar_forward_days: Annotated[
        int,
        typer.Option(help="earnings-calendar 向后覆盖天数。"),
    ] = 90,
    continue_on_failure: Annotated[
        bool,
        typer.Option(
            "--continue-on-failure",
            help=(
                "抓取、写入或 PIT 校验失败时写入失败报告并返回 0，"
                "用于每日调度继续执行后续自带质量门禁的步骤。"
            ),
        ),
    ] = False,
) -> None:
    """抓取 FMP forward-only PIT raw archive 和标准化 as-of 索引。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers) if tickers else load_universe().ai_chain.get("core_watchlist", [])
    )
    fetch_report_output = output_path or default_fmp_forward_pit_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    normalized_output = normalized_output_path or default_fmp_forward_pit_normalized_path(
        DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
        fetch_date,
    )
    pit_report_output = pit_validation_report_path or default_pit_snapshot_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )

    api_key = os.getenv(api_key_env)
    if not api_key:
        fetch_report = build_fmp_forward_pit_failure_report(
            selected_tickers,
            fetch_date,
            code="fmp_forward_pit_api_key_missing",
            message=f"未找到环境变量 {api_key_env}，无法抓取 FMP PIT。",
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
            include_normalized_rows=False,
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="credential",
        )
        return

    try:
        data_sources = load_data_sources(data_sources_path)
    except Exception as exc:
        fetch_report = build_fmp_forward_pit_failure_report(
            selected_tickers,
            fetch_date,
            code="fmp_forward_pit_data_sources_failed",
            message=(
                "FMP PIT 数据源目录加载失败：" f"{sanitize_fmp_forward_pit_error_message(exc)}"
            ),
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="config",
        )
        return

    try:
        fetch_report = fetch_fmp_forward_pit_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
        )
    except ValueError as exc:
        fetch_report = build_fmp_forward_pit_failure_report(
            selected_tickers,
            fetch_date,
            code="fmp_forward_pit_parameter_error",
            message=f"FMP PIT 参数错误：{sanitize_fmp_forward_pit_error_message(exc)}",
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="parameter",
        )
        return
    except Exception as exc:
        fetch_report = build_fmp_forward_pit_failure_report(
            selected_tickers,
            fetch_date,
            code="fmp_forward_pit_unhandled_fetch_error",
            message=(
                "FMP PIT 抓取阶段发生未捕获异常：" f"{sanitize_fmp_forward_pit_error_message(exc)}"
            ),
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
            earnings_calendar_lookback_days=earnings_calendar_lookback_days,
            earnings_calendar_forward_days=earnings_calendar_forward_days,
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="fetch",
        )
        return

    if not fetch_report.passed:
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="fetch",
        )
        return

    try:
        raw_paths = write_fmp_forward_pit_raw_payloads(fetch_report.raw_payloads, raw_output_dir)
        attached_payloads = attach_fmp_forward_pit_raw_paths(fetch_report.raw_payloads, raw_paths)
        fetch_report = attach_fmp_forward_pit_report_artifacts(
            fetch_report,
            raw_payloads=attached_payloads,
            normalized_rows=fetch_report.normalized_rows,
        )
        write_fmp_forward_pit_normalized_csv_from_payloads(
            attached_payloads,
            normalized_output,
        )
        write_fmp_forward_pit_fetch_report(fetch_report, fetch_report_output)

        manifest_records = discover_existing_pit_raw_snapshots(
            fmp_analyst_history_dir=DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
            fmp_historical_valuation_dir=DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR,
            eodhd_earnings_trends_dir=DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR,
            fmp_forward_pit_dir=raw_output_dir,
            data_sources=data_sources,
        )
        manifest_output = write_pit_snapshot_manifest(manifest_records, manifest_path)
        pit_report = validate_pit_snapshot_manifest(
            input_path=manifest_output,
            as_of=fetch_date,
            data_sources=data_sources,
        )
        write_pit_snapshot_validation_report(pit_report, pit_report_output)
    except Exception as exc:
        fetch_report = _append_fmp_forward_pit_failure_issue(
            fetch_report,
            code="fmp_forward_pit_artifact_stage_failed",
            message=(
                "FMP PIT artifact 写入、manifest 刷新或校验阶段失败："
                f"{sanitize_fmp_forward_pit_error_message(exc)}"
            ),
        )
        _finish_fmp_forward_pit_failure(
            fetch_report,
            fetch_report_output,
            continue_on_failure=continue_on_failure,
            stage="artifact",
        )
        return

    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(f"[{status_style}]FMP PIT 抓取状态：{fetch_report.status}[/{status_style}]")
    console.print(f"抓取报告：{fetch_report_output}")
    console.print(f"写入 raw payload：{len(raw_paths)} 个文件 -> {raw_output_dir}")
    console.print(f"写入 normalized CSV：{normalized_output}")
    console.print(f"刷新 PIT manifest：{manifest_output}")
    console.print(f"PIT 快照质量报告：{pit_report_output}")
    console.print(
        f"原始记录：{fetch_report.row_count}；标准化行：{fetch_report.normalized_row_count}"
    )
    console.print(
        f"PIT manifest 状态：{pit_report.status}；"
        f"错误数：{pit_report.error_count}；警告数：{pit_report.warning_count}"
    )
    if not pit_report.passed:
        if continue_on_failure:
            console.print(
                "[yellow]PIT manifest 未通过；已保留报告并继续后续流程。"
                "失败快照不得作为可用 PIT 输入。[/yellow]"
            )
            return
        raise typer.Exit(code=1)


def _append_fmp_forward_pit_failure_issue(
    report: FmpForwardPitFetchReport,
    *,
    code: str,
    message: str,
) -> FmpForwardPitFetchReport:
    issue = FmpForwardPitIssue(
        severity=FmpForwardPitIssueSeverity.ERROR,
        code=code,
        message=message,
    )
    return replace(report, issues=(*report.issues, issue))


def _finish_fmp_forward_pit_failure(
    report: FmpForwardPitFetchReport,
    output_path: Path,
    *,
    continue_on_failure: bool,
    stage: str,
) -> None:
    try:
        write_fmp_forward_pit_fetch_report(report, output_path)
        report_written = True
    except Exception as exc:
        report_written = False
        console.print(
            "[red]FMP PIT 失败报告写入失败：" f"{sanitize_fmp_forward_pit_error_message(exc)}[/red]"
        )

    console.print(f"[red]FMP PIT 抓取状态：{report.status}[/red]")
    console.print(f"失败阶段：{stage}")
    if report_written:
        console.print(f"抓取报告：{output_path}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if continue_on_failure:
        console.print(
            "[yellow]已启用 --continue-on-failure：本步骤不会阻断后续流程；"
            "后续命令仍必须执行自己的质量门禁，失败 PIT 不得作为可用输入。[/yellow]"
        )
        return
    raise typer.Exit(code=1)
