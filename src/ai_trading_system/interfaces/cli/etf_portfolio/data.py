from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.data import (
    ingest_yfinance_prices,
    load_standard_prices,
    read_price_frame,
    standardize_price_frame,
    validate_price_data,
    write_quality_report,
)
from ai_trading_system.etf_portfolio.features import build_feature_store, write_feature_store
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_FEATURE_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REPORT_DIR,
    load_etf_config_bundle,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    parse_date,
    resolve_date,
    satellite_symbols,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import data_app, features_app


@data_app.command("ingest")
def data_ingest_command(
    symbols: Annotated[list[str] | None, typer.Option("--symbols", help="ETF symbols。")] = None,
    start: Annotated[str, typer.Option(help="开始日期 YYYY-MM-DD。")] = "2015-01-01",
    end: Annotated[str | None, typer.Option(help="结束日期 YYYY-MM-DD，默认今天。")] = None,
    output_path: Annotated[Path, typer.Option(help="输出价格缓存路径。")] = (
        PROJECT_ROOT / "data" / "etf_portfolio" / "prices.csv"
    ),
) -> None:
    """从 yfinance 下载 ETF P0 价格并写入标准缓存。"""
    config = load_etf_config_bundle()
    selected_symbols = symbols or list(config.assets.tradeable_symbols)
    output = ingest_yfinance_prices(
        symbols=selected_symbols,
        start=parse_date(start),
        end=parse_date(end) if end else date.today(),
        output_path=output_path,
    )
    typer.echo(f"ETF 价格缓存已写入：{output}")


@data_app.command("validate")
def data_validate_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--date", "--as-of", help="评估日期。")] = None,
    output_path: Annotated[Path | None, typer.Option(help="质量报告输出路径。")] = None,
) -> None:
    """校验 ETF price cache。"""
    config = load_etf_config_bundle()
    run_date = resolve_date(as_of, prices_path=prices_path)
    raw = read_price_frame(prices_path)
    prices, metadata_issues = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name=str(prices_path),
    )
    report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=run_date,
        extra_issues=metadata_issues,
    )
    report_path = output_path or DEFAULT_ETF_REPORT_DIR / f"data_quality_{run_date.isoformat()}.md"
    write_quality_report(report, report_path)
    typer.echo(f"ETF 数据质量状态：{report.status}")
    typer.echo(f"报告：{report_path}")
    if not report.passed:
        raise typer.Exit(code=1)


@features_app.command("build")
def features_build_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    output_path: Annotated[Path, typer.Option(help="Feature store 输出路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    start: Annotated[str | None, typer.Option(help="特征构建开始日期。")] = None,
    end: Annotated[str | None, typer.Option(help="特征构建结束日期。")] = None,
    include_satellites: Annotated[
        bool,
        typer.Option("--include-satellites", help="包含 P1 satellite stock optional features。"),
    ] = False,
) -> None:
    """构建 ETF feature store；先执行同一数据质量门禁。"""
    config = load_etf_config_bundle()
    prices, report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=satellite_symbols(config) if include_satellites else None,
    )
    if not report.passed:
        typer.echo(f"ETF 数据质量状态：{report.status}，已停止 feature build。")
        raise typer.Exit(code=1)
    features = build_feature_store(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        start=parse_date(start) if start else None,
        end=resolve_date(end, prices=prices) if end else None,
    )
    write_feature_store(features, output_path)
    typer.echo(f"ETF feature store 已写入：{output_path}（{len(features)} 行）")
    typer.echo(f"data_quality_status={report.status}")


__all__ = ["data_ingest_command", "data_validate_command", "features_build_command"]
