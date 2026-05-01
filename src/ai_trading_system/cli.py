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
    load_portfolio,
    load_scoring_rules,
    load_universe,
)
from ai_trading_system.data.download import download_daily_data
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.features.market import (
    build_market_features,
    default_feature_report_path,
    write_feature_summary,
    write_features_csv,
)
from ai_trading_system.reports.daily import render_recommendation_markdown
from ai_trading_system.scoring.daily import (
    build_daily_score_report,
    default_daily_score_report_path,
    write_daily_score_report,
    write_scores_csv,
)
from ai_trading_system.scoring.position_model import ModuleScore, WeightedScoreModel

app = typer.Typer(help="AI 产业链趋势分析和仓位管理工具。", no_args_is_help=True)
console = Console()


@app.callback()
def main() -> None:
    """AI 产业链趋势分析和仓位管理工具。"""


@app.command("score-example")
def score_example() -> None:
    """输出一份示例仓位建议。"""
    model = WeightedScoreModel()
    portfolio = load_portfolio()
    recommendation = model.recommend(
        [
            ModuleScore(
                "trend",
                score=72,
                weight=25,
                reason="SMH 和 QQQ 仍在长期均线上方",
            ),
            ModuleScore("fundamentals", score=60, weight=25, reason="MVP 阶段中性占位"),
            ModuleScore("macro_liquidity", score=55, weight=15, reason="利率环境不算友好"),
            ModuleScore("risk_sentiment", score=66, weight=15, reason="VIX 仍可控"),
            ModuleScore("valuation", score=50, weight=10, reason="MVP 阶段中性占位"),
            ModuleScore(
                "policy_geopolitics",
                score=60,
                weight=10,
                reason="MVP 阶段中性占位",
            ),
        ],
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
    )
    console.print(render_recommendation_markdown(recommendation))


@app.command("download-data")
def download_data(
    start: Annotated[
        str,
        typer.Option(help="开始日期，包含当天，格式为 YYYY-MM-DD。"),
    ] = "2018-01-01",
    end: Annotated[
        str | None,
        typer.Option(help="结束日期，包含当天，格式为 YYYY-MM-DD。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出缓存目录。"),
    ] = PROJECT_ROOT / "data" / "raw",
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="包含配置中的完整 AI 产业链标的，而不只下载核心观察池。",
        ),
    ] = False,
) -> None:
    """下载市场日线价格和 FRED 利率数据到本地 CSV 缓存。"""
    universe = load_universe()
    start_date = _parse_date(start)
    end_date = _parse_date(end) if end else date.today()

    summary = download_daily_data(
        universe,
        start=start_date,
        end=end_date,
        output_dir=output_dir,
        include_full_ai_chain=full_universe,
    )

    console.print("[green]数据缓存已更新。[/green]")
    console.print(f"价格数据：{summary.prices_path}（{summary.price_rows} 行）")
    console.print(f"利率数据：{summary.rates_path}（{summary.rate_rows} 行）")
    console.print(f"价格标的：{', '.join(summary.price_tickers)}")
    console.print(f"利率序列：{', '.join(summary.rate_series)}")


@app.command("validate-data")
def validate_data(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """校验缓存数据并写入 Markdown 质量报告。"""
    universe = load_universe()
    quality_config = load_data_quality()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=validation_date,
    )
    write_data_quality_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据质量状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@app.command("build-features")
def build_features(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="特征日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "features_daily.csv",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 特征摘要输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
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

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=feature_date,
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

    feature_set = build_market_features(
        prices=pd.read_csv(prices_path),
        rates=pd.read_csv(rates_path),
        config=feature_config,
        as_of=feature_date,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
    )
    features_output = write_features_csv(feature_set, output_path)
    feature_summary_output = write_feature_summary(
        feature_set,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        features_path=features_output,
        output_path=feature_report_output,
    )

    status_style = "green" if feature_set.status == "PASS" else "yellow"
    console.print(f"[{status_style}]特征构建状态：{feature_set.status}[/{status_style}]")
    console.print(f"特征数据：{features_output}（{feature_date} 共 {len(feature_set.rows)} 行）")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(f"特征警告数：{len(feature_set.warnings)}")


@app.command("score-daily")
def score_daily(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评分日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    features_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "features_daily.csv",
    scores_path: Annotated[
        Path,
        typer.Option(help="评分 CSV 输出路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日评分报告输出路径。"),
    ] = None,
    feature_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 特征摘要输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验并评分配置中的完整 AI 产业链标的。",
        ),
    ] = False,
) -> None:
    """构建特征并生成每日市场评分报告。"""
    universe = load_universe()
    data_quality_config = load_data_quality()
    feature_config = load_features()
    scoring_rules = load_scoring_rules()
    portfolio = load_portfolio()
    score_date = _parse_date(as_of) if as_of else date.today()
    expected_price_tickers = configured_price_tickers(
        universe,
        include_full_ai_chain=full_universe,
    )
    expected_rate_series = configured_rate_series(universe)
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    feature_report_output = feature_report_path or default_feature_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    score_report_output = report_path or default_daily_score_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=score_date,
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止每日评分。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    feature_set = build_market_features(
        prices=pd.read_csv(prices_path),
        rates=pd.read_csv(rates_path),
        config=feature_config,
        as_of=score_date,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
    )
    features_output = write_features_csv(feature_set, features_path)
    feature_summary_output = write_feature_summary(
        feature_set,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        features_path=features_output,
        output_path=feature_report_output,
    )
    score_report = build_daily_score_report(
        feature_set=feature_set,
        data_quality_report=data_quality_report,
        rules=scoring_rules,
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
    )
    scores_output = write_scores_csv(score_report, scores_path)
    daily_report_output = write_daily_score_report(
        score_report,
        data_quality_report_path=quality_output,
        feature_report_path=feature_summary_output,
        features_path=features_output,
        scores_path=scores_output,
        output_path=score_report_output,
    )

    status_style = "green" if score_report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]每日评分状态：{score_report.status}[/{status_style}]")
    console.print(f"总分：{score_report.recommendation.total_score:.1f}")
    console.print(f"仓位状态：{score_report.recommendation.label}")
    console.print(f"每日评分报告：{daily_report_output}")
    console.print(f"评分数据：{scores_output}")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


if __name__ == "__main__":
    app()
