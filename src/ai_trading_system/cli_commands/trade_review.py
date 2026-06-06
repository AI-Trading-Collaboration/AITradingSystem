from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console

from ai_trading_system.backtest.daily import DEFAULT_BENCHMARK_TICKERS
from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
    load_watchlist,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.trade_review import (
    build_trade_review_report,
    default_trade_review_report_path,
    load_trade_record_store,
    validate_trade_record_store,
    write_trade_review_report,
)

console = Console()


def register_trade_review_commands(app: typer.Typer) -> None:
    app.command("review-trades")(review_trades)


def review_trades(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    input_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trades",
    as_of: Annotated[
        str | None,
        typer.Option(help="复盘日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的归因基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 交易复盘报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """通过数据质量门禁后复盘交易记录并做基础基准归因。"""
    universe = load_universe()
    review_date = _parse_date(as_of) if as_of else date.today()
    benchmark_tickers = _parse_csv_items(benchmarks)
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个归因基准标的。")

    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    report_output = output_path or default_trade_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=review_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止交易复盘。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    validation_report = validate_trade_record_store(
        store=load_trade_record_store(input_path),
        universe=universe,
        watchlist=load_watchlist(),
        as_of=review_date,
    )
    review_report = build_trade_review_report(
        validation_report=validation_report,
        prices=pd.read_csv(prices_path),
        data_quality_report=data_quality_report,
        benchmark_tickers=tuple(benchmark_tickers),
    )
    write_trade_review_report(
        review_report,
        data_quality_report_path=quality_output,
        output_path=report_output,
    )

    status_style = (
        "green"
        if review_report.status == "PASS"
        else ("yellow" if validation_report.passed else "red")
    )
    console.print(f"[{status_style}]交易复盘状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"交易数：{validation_report.trade_count}")
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")

    if not validation_report.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


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
