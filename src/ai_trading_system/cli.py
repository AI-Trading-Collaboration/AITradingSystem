from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from ai_trading_system.backtest.daily import (
    DEFAULT_BENCHMARK_TICKERS,
    BacktestRegimeContext,
    default_backtest_daily_path,
    default_backtest_report_path,
    run_daily_score_backtest,
    write_backtest_daily_csv,
    write_backtest_report,
)
from ai_trading_system.config import (
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_features,
    load_industry_chain,
    load_market_regimes,
    load_portfolio,
    load_scoring_rules,
    load_universe,
    load_watchlist,
    market_regime_by_id,
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
from ai_trading_system.industry_chain import (
    default_industry_chain_report_path,
    validate_industry_chain_config,
    write_industry_chain_validation_report,
)
from ai_trading_system.reports.daily import render_recommendation_markdown
from ai_trading_system.scoring.daily import (
    build_daily_score_report,
    default_daily_score_report_path,
    write_daily_score_report,
    write_scores_csv,
)
from ai_trading_system.scoring.position_model import ModuleScore, WeightedScoreModel
from ai_trading_system.watchlist import (
    default_watchlist_report_path,
    validate_watchlist_config,
    write_watchlist_validation_report,
)

app = typer.Typer(help="AI 产业链趋势分析和仓位管理工具。", no_args_is_help=True)
watchlist_app = typer.Typer(help="观察池和能力圈管理。", no_args_is_help=True)
industry_chain_app = typer.Typer(help="产业链节点和因果图管理。", no_args_is_help=True)
app.add_typer(watchlist_app, name="watchlist")
app.add_typer(industry_chain_app, name="industry-chain")
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


@app.command("backtest")
def backtest(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    start: Annotated[
        str | None,
        typer.Option(
            "--from",
            help="回测开始日期，格式为 YYYY-MM-DD；未提供时使用所选市场阶段起点。",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--to", help="回测结束日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="用于承载 AI 仓位的策略代理标的。"),
    ] = "SMH",
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的买入持有基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    cost_bps: Annotated[
        float,
        typer.Option(help="单边交易成本，单位 bps。"),
    ] = 5.0,
    regime: Annotated[
        str | None,
        typer.Option(
            "--regime",
            help="市场阶段 ID，默认使用 config/market_regimes.yaml 的 default_backtest_regime。",
        ),
    ] = None,
    regimes_path: Annotated[
        Path,
        typer.Option(help="市场阶段配置文件路径。"),
    ] = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    daily_output_path: Annotated[
        Path | None,
        typer.Option(help="每日回测明细 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 回测报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    quality_as_of: Annotated[
        str | None,
        typer.Option(help="数据质量校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """基于每日评分规则运行历史回测。"""
    universe = load_universe()
    data_quality_config = load_data_quality()
    feature_config = load_features()
    scoring_rules = load_scoring_rules()
    portfolio = load_portfolio()
    market_regimes = load_market_regimes(regimes_path)
    selected_regime_id = regime or market_regimes.default_backtest_regime
    try:
        selected_regime = market_regime_by_id(market_regimes, selected_regime_id)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    start_date = _parse_date(start) if start else selected_regime.start_date
    end_date = _parse_date(end) if end else date.today()
    quality_date = _parse_date(quality_as_of) if quality_as_of else date.today()
    benchmark_tickers = _parse_csv_items(benchmarks)
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个基准标的。")

    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        quality_date,
    )
    backtest_daily_output = daily_output_path or default_backtest_daily_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_report_output = report_path or default_backtest_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=data_quality_config,
        as_of=quality_date,
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止回测。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    result = run_daily_score_backtest(
        prices=pd.read_csv(prices_path),
        rates=pd.read_csv(rates_path),
        feature_config=feature_config,
        scoring_rules=scoring_rules,
        portfolio_config=portfolio,
        data_quality_report=data_quality_report,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
        start=start_date,
        end=end_date,
        strategy_ticker=strategy_ticker,
        benchmark_tickers=tuple(benchmark_tickers),
        cost_bps=cost_bps,
        market_regime=BacktestRegimeContext(
            regime_id=selected_regime.regime_id,
            name=selected_regime.name,
            start_date=selected_regime.start_date,
            anchor_date=selected_regime.anchor_date,
            anchor_event=selected_regime.anchor_event,
            description=selected_regime.description,
        ),
    )
    daily_output = write_backtest_daily_csv(result, backtest_daily_output)
    report_output = write_backtest_report(
        result,
        data_quality_report_path=quality_output,
        daily_output_path=daily_output,
        output_path=backtest_report_output,
    )

    console.print(f"[yellow]回测状态：{result.status}[/yellow]")
    if result.market_regime is not None:
        console.print(
            f"市场阶段：{result.market_regime.name}（{result.market_regime.regime_id}）"
        )
    console.print(f"策略总收益：{result.strategy_metrics.total_return:.1%}")
    console.print(f"策略 CAGR：{result.strategy_metrics.cagr:.1%}")
    console.print(f"策略最大回撤：{result.strategy_metrics.max_drawdown:.1%}")
    console.print(f"回测报告：{report_output}")
    console.print(f"每日明细：{daily_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


@watchlist_app.command("list")
def list_watchlist(
    config_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃标的，或显示全部配置标的。"),
    ] = True,
) -> None:
    """列出观察池和能力圈配置。"""
    watchlist = load_watchlist(config_path)
    items = [item for item in watchlist.items if item.active or not active_only]

    table = Table(title="观察池与能力圈")
    table.add_column("Ticker")
    table.add_column("公司")
    table.add_column("类型")
    table.add_column("能力圈")
    table.add_column("风险")
    table.add_column("Thesis")
    table.add_column("产业链节点")

    for item in sorted(items, key=lambda value: value.ticker):
        table.add_row(
            item.ticker,
            item.company_name,
            item.instrument_type,
            f"{item.competence_score:.0f}",
            _risk_level_label(item.default_risk_level),
            "需要" if item.thesis_required else "不需要",
            ", ".join(item.ai_chain_nodes),
        )

    console.print(table)


@watchlist_app.command("validate")
def validate_watchlist(
    config_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 观察池校验报告输出路径。"),
    ] = None,
) -> None:
    """校验观察池覆盖、能力圈和 thesis 约束。"""
    universe = load_universe()
    watchlist = load_watchlist(config_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_watchlist_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_watchlist_config(
        watchlist=watchlist,
        universe=universe,
        as_of=validation_date,
    )
    write_watchlist_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]观察池校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"活跃标的数：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@industry_chain_app.command("list")
def list_industry_chain(
    config_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
) -> None:
    """列出产业链节点和因果关系。"""
    industry_chain = load_industry_chain(config_path)

    table = Table(title="产业链因果图")
    table.add_column("节点")
    table.add_column("名称")
    table.add_column("父节点")
    table.add_column("周期")
    table.add_column("现金流")
    table.add_column("情绪")
    table.add_column("相关标的")

    for node in sorted(industry_chain.nodes, key=lambda value: value.node_id):
        table.add_row(
            node.node_id,
            node.name,
            ", ".join(node.parent_node_ids) or "无",
            _horizon_label(node.impact_horizon),
            _relevance_label(node.cash_flow_relevance),
            _relevance_label(node.sentiment_relevance),
            ", ".join(node.related_tickers),
        )

    console.print(table)


@industry_chain_app.command("validate")
def validate_industry_chain(
    config_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验节点引用。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 产业链校验报告输出路径。"),
    ] = None,
) -> None:
    """校验产业链节点、父子关系和观察池引用。"""
    industry_chain = load_industry_chain(config_path)
    watchlist = load_watchlist(watchlist_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_industry_chain_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_industry_chain_config(
        industry_chain=industry_chain,
        watchlist=watchlist,
        as_of=validation_date,
    )
    write_industry_chain_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]产业链校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"节点数：{len(report.nodes)}")
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


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _risk_level_label(level: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
        "critical": "极高",
    }.get(level, level)


def _horizon_label(value: str) -> str:
    return {
        "short": "短期",
        "medium": "中期",
        "long": "长期",
    }.get(value, value)


def _relevance_label(value: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
    }.get(value, value)


if __name__ == "__main__":
    app()
