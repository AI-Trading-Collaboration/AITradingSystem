from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ai_trading_system.config import (
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_data_sources,
    load_universe,
)
from ai_trading_system.data.market_data import YFinancePriceProvider
from ai_trading_system.data.quality import validate_data_cache
from ai_trading_system.data_source_fallback_policy import DEFAULT_DATA_SOURCE_FALLBACK_DIR
from ai_trading_system.data_sources import (
    build_data_source_health_report,
    default_data_source_health_report_path,
    default_data_sources_report_path,
    validate_data_sources_config,
    write_data_source_health_report,
    write_data_sources_validation_report,
)
from ai_trading_system.pit_source_manifest import (
    DEFAULT_PIT_SOURCE_MANIFEST_DIR,
    build_and_write_pit_source_manifest,
    load_pit_source_manifest_payload,
    resolve_pit_source_manifest_path,
    validate_pit_source_manifest_artifact,
)
from ai_trading_system.price_source_diagnostics import (
    build_yahoo_price_diagnostic_report,
    default_yahoo_price_diagnostic_report_path,
    write_yahoo_price_diagnostic_report,
)

console = Console()
data_sources_app = typer.Typer(help="数据源目录和审计规则管理。", no_args_is_help=True)
pit_manifest_app = typer.Typer(help="Point-in-time source manifest 治理报告。")
data_sources_app.add_typer(pit_manifest_app, name="pit-manifest")


@data_sources_app.command("list")
def list_data_sources(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃数据源，或显示全部数据源。"),
    ] = False,
) -> None:
    """列出数据源目录。"""
    data_sources = load_data_sources(config_path)

    table = Table(title="数据源目录")
    table.add_column("Source", overflow="fold")
    table.add_column("Provider", overflow="fold")
    table.add_column("类型")
    table.add_column("状态")
    table.add_column("领域", overflow="fold")
    table.add_column("缓存", overflow="fold")
    for source in sorted(data_sources.sources, key=lambda item: item.source_id):
        if active_only and source.status != "active":
            continue
        table.add_row(
            source.source_id,
            source.provider,
            _data_source_type_label(source.source_type),
            _data_source_status_label(source.status),
            ", ".join(source.domains),
            ", ".join(source.cache_paths),
        )
    console.print(table)


@data_sources_app.command("validate")
def validate_data_sources(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据源目录校验报告输出路径。"),
    ] = None,
) -> None:
    """校验数据源目录、审计字段和来源限制。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_data_sources_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_data_sources_config(
        config=load_data_sources(config_path),
        as_of=validation_date,
    )
    write_data_sources_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据源目录校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"数据源数量：{len(report.sources)}；活跃：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@data_sources_app.command("health")
def data_source_health(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option(help="download_manifest.csv 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "download_manifest.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据源健康报告输出路径。"),
    ] = None,
) -> None:
    """生成 provider health 和 reconciliation 覆盖报告。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_data_source_health_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    report = build_data_source_health_report(
        config=load_data_sources(config_path),
        as_of=evaluation_date,
        manifest_path=manifest_path,
    )
    write_data_source_health_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据源健康状态：{report.status}[/{status_style}]")
    console.print(f"Provider health score：{report.health_score}")
    console.print(f"报告：{report_path}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@data_sources_app.command("yahoo-price-diagnostic")
def yahoo_price_diagnostic(
    prices_path: Annotated[
        Path,
        typer.Option(help="FMP 主价格缓存 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="FRED 宏观序列 CSV 路径，用于复用数据质量报告。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    marketstack_prices_path: Annotated[
        Path | None,
        typer.Option(help="Marketstack 第二行情源 CSV 路径；默认跟随主价格缓存目录。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="诊断日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown Yahoo 诊断报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="复用完整 AI 产业链标的的数据质量上下文。",
        ),
    ] = False,
    window_days: Annotated[
        int,
        typer.Option(help="围绕异常 ticker/date 拉取 Yahoo 样本的前后自然日窗口。"),
    ] = 3,
    max_targets: Annotated[
        int,
        typer.Option(help="最多复查的 Marketstack 异常 ticker/date 数量。"),
    ] = 20,
) -> None:
    """对 Marketstack 自检坏行执行只读 Yahoo 诊断复查。"""
    universe = load_universe()
    quality_config = load_data_quality()
    diagnostic_date = _parse_date(as_of) if as_of else date.today()
    secondary_path = marketstack_prices_path or _marketstack_prices_path(prices_path)
    report_path = output_path or default_yahoo_price_diagnostic_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        diagnostic_date,
    )
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=diagnostic_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=secondary_path,
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    diagnostic_report = build_yahoo_price_diagnostic_report(
        primary_prices_path=prices_path,
        marketstack_prices_path=secondary_path,
        quality_report=quality_report,
        quality_config=quality_config,
        yahoo_provider=YFinancePriceProvider(),
        as_of=diagnostic_date,
        window_days=window_days,
        max_targets=max_targets,
    )
    write_yahoo_price_diagnostic_report(diagnostic_report, report_path)

    status_style = (
        "green"
        if diagnostic_report.status == "PASS"
        else "yellow" if diagnostic_report.status != "DIAGNOSTIC_FAILED" else "red"
    )
    console.print(
        f"[{status_style}]Yahoo 价格诊断状态：" f"{diagnostic_report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(f"诊断目标：{len(diagnostic_report.targets)}")
    console.print(f"Yahoo 返回行数：{diagnostic_report.row_count}")
    console.print("治理边界：diagnostic only / production_effect=none，不写价格缓存或评分。")


@pit_manifest_app.command("report")
def pit_source_manifest_report(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    download_manifest_path: Annotated[
        Path,
        typer.Option(help="download_manifest.csv 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "download_manifest.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="PIT source manifest artifact 根目录。"),
    ] = DEFAULT_PIT_SOURCE_MANIFEST_DIR,
    fallback_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="显式 fallback policy report JSON 路径；缺省读取 latest。"),
    ] = None,
    fallback_policy_output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="只读取 latest artifact，不生成新 manifest。"),
    ] = False,
) -> None:
    """生成或读取 source-level PIT source manifest。"""
    if latest:
        manifest_path = resolve_pit_source_manifest_path(latest=True, output_dir=output_dir)
        payload = load_pit_source_manifest_payload(manifest_path)
        paths = {"manifest_json": manifest_path}
    else:
        evaluation_date = _parse_date(as_of) if as_of else date.today()
        payload, paths = build_and_write_pit_source_manifest(
            config=load_data_sources(config_path),
            as_of=evaluation_date,
            download_manifest_path=download_manifest_path,
            output_dir=output_dir,
            fallback_policy_report_path=fallback_policy_report_path,
            fallback_policy_output_dir=fallback_policy_output_dir,
        )

    summary = payload.get("summary", {})
    status = str(payload.get("status", "UNKNOWN"))
    status_style = "green" if status == "PASS" else "yellow" if status != "FAIL" else "red"
    console.print(f"[{status_style}]PIT source manifest status={status}[/{status_style}]")
    console.print(f"manifest_id={payload.get('manifest_id')}")
    console.print(f"source_count={summary.get('source_count')}")
    console.print(
        "grade_counts="
        f"STRONG_PIT:{summary.get('strong_pit_count')}, "
        f"APPROX_PIT:{summary.get('approx_pit_count')}, "
        f"NON_PIT:{summary.get('non_pit_count')}, "
        f"UNKNOWN:{summary.get('unknown_count')}"
    )
    console.print(f"non_strong_source_count={summary.get('non_strong_source_count')}")
    console.print(f"validation_status={payload.get('validation_status')}")
    console.print(f"report={paths.get('manifest_json')}")
    console.print(
        "production_effect=none；只读治理报告，不刷新数据、不运行评分/回测、不触发 broker。"
    )

    if status == "FAIL":
        raise typer.Exit(code=1)


@pit_manifest_app.command("validate")
def validate_pit_source_manifest(
    manifest_id: Annotated[
        str | None,
        typer.Option(help="要校验的 manifest_id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="校验 latest PIT source manifest。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="PIT source manifest artifact 根目录。"),
    ] = DEFAULT_PIT_SOURCE_MANIFEST_DIR,
) -> None:
    """校验 source-level PIT source manifest schema、grade 和安全边界。"""
    validation, manifest_path = validate_pit_source_manifest_artifact(
        manifest_id=manifest_id,
        latest=latest or manifest_id is None,
        output_dir=output_dir,
    )
    status_style = (
        "green" if validation.status == "PASS" else "yellow" if validation.passed else "red"
    )
    console.print(
        f"[{status_style}]PIT source manifest validation status={validation.status}"
        f"[/{status_style}]"
    )
    console.print(f"manifest_id={validation.manifest_id}")
    console.print(f"manifest={manifest_path}")
    console.print(f"source_count={validation.source_count}")
    console.print(f"error_count={validation.error_count}; warning_count={validation.warning_count}")
    console.print("production_effect=none；校验只读 existing artifact。")

    if not validation.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc


def _download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


def _marketstack_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / "prices_marketstack_daily.csv"


def _requires_marketstack_prices(prices_path: Path) -> bool:
    default_prices_path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    try:
        return prices_path.resolve() == default_prices_path.resolve()
    except OSError:
        return prices_path == default_prices_path


def _data_source_type_label(value: str) -> str:
    return {
        "primary_source": "一手来源",
        "paid_vendor": "付费供应商",
        "public_convenience": "公开便利源",
        "manual_input": "手工输入",
    }.get(value, value)


def _data_source_status_label(value: str) -> str:
    return {
        "active": "已启用",
        "planned": "计划中",
        "inactive": "停用",
    }.get(value, value)
