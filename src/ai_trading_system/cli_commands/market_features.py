from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_features,
    load_universe,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.feature_availability import (
    DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    FeatureAvailabilitySourceCheck,
    build_feature_availability_report,
    build_feature_source_check,
    default_feature_availability_report_path,
    render_feature_availability_section,
    write_feature_availability_report,
)
from ai_trading_system.features.market import (
    build_market_features,
    default_feature_report_path,
    write_feature_summary,
    write_features_csv,
)

console = Console()


def register_market_feature_commands(app: typer.Typer) -> None:
    app.command("build-features")(build_features)


def build_features(
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
    as_of: Annotated[
        str | None,
        typer.Option(help="特征日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "features_daily.csv",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 特征摘要输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    feature_availability_path: Annotated[
        Path,
        typer.Option(help="PIT feature availability catalog YAML 路径。"),
    ] = DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    feature_availability_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 特征可见时间报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验并构建配置中的完整 AI 产业链标的特征。",
        ),
    ] = False,
) -> None:
    """通过数据质量门禁后构建每日市场特征。"""
    universe = load_universe()
    data_quality_config = load_data_quality()
    feature_config = load_features()
    feature_date = _parse_date(as_of) if as_of else date.today()
    expected_price_tickers = configured_price_tickers(
        universe,
        include_full_ai_chain=full_universe,
    )
    expected_rate_series = configured_rate_series(universe)
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )
    feature_report_output = report_path or default_feature_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )
    feature_availability_output = (
        feature_availability_report_path
        or default_feature_availability_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            feature_date,
        )
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=feature_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止特征构建。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    rates_frame = pd.read_csv(rates_path)
    feature_set = build_market_features(
        prices=prices_frame,
        rates=rates_frame,
        config=feature_config,
        as_of=feature_date,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
    )
    feature_availability_report = build_feature_availability_report(
        input_path=feature_availability_path,
        as_of=feature_date,
        observed_sources=tuple(sorted({row.source for row in feature_set.rows})),
        required_sources=("prices_daily", "rates_daily"),
        source_checks=_market_feature_source_checks(
            prices_frame=prices_frame,
            rates_frame=rates_frame,
            prices_path=prices_path,
            rates_path=rates_path,
            decision_time=feature_date,
        ),
    )
    write_feature_availability_report(
        feature_availability_report,
        feature_availability_output,
    )
    if not feature_availability_report.passed:
        console.print("[red]PIT 特征可见时间校验失败，已停止特征构建。[/red]")
        console.print(f"PIT 特征可见时间报告：{feature_availability_output}")
        console.print(
            f"错误数：{feature_availability_report.error_count}；"
            f"警告数：{feature_availability_report.warning_count}"
        )
        raise typer.Exit(code=1)
    features_output = write_features_csv(feature_set, output_path)
    feature_summary_output = write_feature_summary(
        feature_set,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        features_path=features_output,
        output_path=feature_report_output,
        feature_availability_section=render_feature_availability_section(
            feature_availability_report,
            feature_availability_output,
        ),
    )

    status_style = "green" if feature_set.status == "PASS" else "yellow"
    console.print(f"[{status_style}]特征构建状态：{feature_set.status}[/{status_style}]")
    console.print(f"特征数据：{features_output}（{feature_date} 共 {len(feature_set.rows)} 行）")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(
        f"PIT 特征可见时间报告：{feature_availability_output}"
        f"（{feature_availability_report.status}）"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(f"特征警告数：{len(feature_set.warnings)}")


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


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


def _market_feature_source_checks(
    *,
    prices_frame: pd.DataFrame,
    rates_frame: pd.DataFrame,
    prices_path: Path,
    rates_path: Path,
    decision_time: date,
) -> tuple[FeatureAvailabilitySourceCheck, ...]:
    return (
        build_feature_source_check(
            source="prices_daily",
            frame=prices_frame,
            decision_time=decision_time,
            input_path=prices_path,
            event_time_columns=("date",),
            available_time_columns=(
                "available_time",
                "vendor_available_at",
                "ingested_at",
                "downloaded_at",
            ),
            fallback_policy="download_manifest.downloaded_at；若缺失则按下一交易日可用保守处理",
        ),
        build_feature_source_check(
            source="rates_daily",
            frame=rates_frame,
            decision_time=decision_time,
            input_path=rates_path,
            event_time_columns=("date",),
            available_time_columns=(
                "available_time",
                "vendor_available_at",
                "ingested_at",
                "downloaded_at",
            ),
            fallback_policy="download_manifest.downloaded_at；若缺失则按下一交易日可用保守处理",
        ),
    )
