from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table
from yaml import YAMLError

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
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_RISK_EVENTS_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    IndustryChainConfig,
    UniverseConfig,
    WatchlistConfig,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_data_sources,
    load_features,
    load_fundamental_features,
    load_fundamental_metrics,
    load_industry_chain,
    load_market_regimes,
    load_portfolio,
    load_risk_events,
    load_scoring_rules,
    load_sec_companies,
    load_universe,
    load_watchlist,
    market_regime_by_id,
)
from ai_trading_system.data.download import download_daily_data
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.data_sources import (
    default_data_sources_report_path,
    validate_data_sources_config,
    write_data_sources_validation_report,
)
from ai_trading_system.features.market import (
    build_market_features,
    default_feature_report_path,
    write_feature_summary,
    write_features_csv,
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
from ai_trading_system.fundamentals.sec_metrics import (
    build_sec_fundamental_metrics_report,
    default_sec_fundamental_metrics_csv_path,
    default_sec_fundamental_metrics_report_path,
    default_sec_fundamental_metrics_validation_report_path,
    validate_sec_fundamental_metrics_csv,
    write_sec_fundamental_metrics_csv,
    write_sec_fundamental_metrics_report,
    write_sec_fundamental_metrics_validation_report,
)
from ai_trading_system.fundamentals.sec_validation import (
    default_sec_companyfacts_validation_report_path,
    validate_sec_companyfacts_cache,
    write_sec_companyfacts_validation_report,
)
from ai_trading_system.industry_chain import (
    default_industry_chain_report_path,
    validate_industry_chain_config,
    write_industry_chain_validation_report,
)
from ai_trading_system.reports.daily import render_recommendation_markdown
from ai_trading_system.risk_events import (
    default_risk_events_report_path,
    validate_risk_events_config,
    write_risk_events_validation_report,
)
from ai_trading_system.scoring.daily import (
    DailyManualReviewStatus,
    DailyReviewSummary,
    build_daily_score_report,
    default_daily_score_report_path,
    write_daily_score_report,
    write_scores_csv,
)
from ai_trading_system.scoring.position_model import ModuleScore, WeightedScoreModel
from ai_trading_system.thesis import (
    build_thesis_review_report,
    default_thesis_review_report_path,
    default_thesis_validation_report_path,
    load_trade_thesis_store,
    validate_trade_thesis_store,
    write_thesis_review_report,
    write_thesis_validation_report,
)
from ai_trading_system.trade_review import (
    build_trade_review_report,
    default_trade_review_report_path,
    load_trade_record_store,
    validate_trade_record_store,
    write_trade_review_report,
)
from ai_trading_system.valuation import (
    build_valuation_review_report,
    default_valuation_review_report_path,
    default_valuation_validation_report_path,
    load_valuation_snapshot_store,
    validate_valuation_snapshot_store,
    write_valuation_review_report,
    write_valuation_validation_report,
)
from ai_trading_system.watchlist import (
    default_watchlist_report_path,
    validate_watchlist_config,
    write_watchlist_validation_report,
)

app = typer.Typer(help="AI 产业链趋势分析和仓位管理工具。", no_args_is_help=True)
watchlist_app = typer.Typer(help="观察池和能力圈管理。", no_args_is_help=True)
industry_chain_app = typer.Typer(help="产业链节点和因果图管理。", no_args_is_help=True)
thesis_app = typer.Typer(help="交易 thesis 和假设验证管理。", no_args_is_help=True)
risk_events_app = typer.Typer(help="风险事件分级和动作规则管理。", no_args_is_help=True)
valuation_app = typer.Typer(help="估值、预期和拥挤度快照管理。", no_args_is_help=True)
data_sources_app = typer.Typer(help="数据源目录和审计规则管理。", no_args_is_help=True)
fundamentals_app = typer.Typer(help="基本面数据源下载和审计。", no_args_is_help=True)
app.add_typer(watchlist_app, name="watchlist")
app.add_typer(industry_chain_app, name="industry-chain")
app.add_typer(thesis_app, name="thesis")
app.add_typer(risk_events_app, name="risk-events")
app.add_typer(valuation_app, name="valuation")
app.add_typer(data_sources_app, name="data-sources")
app.add_typer(fundamentals_app, name="fundamentals")
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
    console.print(f"下载审计清单：{summary.manifest_path}")
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
        manifest_path=_download_manifest_path(prices_path),
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
        manifest_path=_download_manifest_path(prices_path),
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


@thesis_app.command("list")
def list_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
) -> None:
    """列出本地交易 thesis。"""
    store = load_trade_thesis_store(input_path)

    table = Table(title="交易 Thesis")
    table.add_column("Thesis")
    table.add_column("Ticker")
    table.add_column("方向")
    table.add_column("状态")
    table.add_column("创建日期")
    table.add_column("复核频率")
    table.add_column("文件")

    for loaded in sorted(store.loaded, key=lambda item: item.thesis.thesis_id):
        thesis = loaded.thesis
        table.add_row(
            thesis.thesis_id,
            thesis.ticker,
            _thesis_direction_label(thesis.direction),
            _thesis_status_label(thesis.status),
            thesis.created_at.isoformat(),
            _thesis_review_frequency_label(thesis.review_frequency),
            str(loaded.path),
        )

    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的交易 thesis。")
    if store.load_errors:
        console.print(
            f"[red]存在 {len(store.load_errors)} 个加载错误，请运行 validate 查看。[/red]"
        )


@thesis_app.command("validate")
def validate_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown thesis 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验交易 thesis 的结构、引用和复核约束。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_thesis_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    store = load_trade_thesis_store(input_path)
    report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(watchlist_path),
        industry_chain=load_industry_chain(industry_chain_path),
        as_of=validation_date,
    )
    write_thesis_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]交易 thesis 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Thesis 数量：{report.thesis_count}；活跃：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@thesis_app.command("review")
def review_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown thesis 复核报告输出路径。"),
    ] = None,
) -> None:
    """复核交易 thesis 的验证指标、证伪条件和风险事件状态。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_thesis_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    store = load_trade_thesis_store(input_path)
    validation_report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(watchlist_path),
        industry_chain=load_industry_chain(industry_chain_path),
        as_of=review_date,
    )
    review_report = build_thesis_review_report(validation_report)
    write_thesis_review_report(review_report, report_path)

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
    )
    console.print(f"[{status_style}]交易 thesis 复核状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Thesis 数量：{validation_report.thesis_count}；"
        f"活跃：{validation_report.active_count}"
    )
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("list")
def list_risk_events(
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃规则，或显示全部规则。"),
    ] = True,
) -> None:
    """列出风险事件分级规则。"""
    risk_events = load_risk_events(config_path)

    levels_table = Table(title="风险等级")
    levels_table.add_column("等级")
    levels_table.add_column("名称")
    levels_table.add_column("AI 仓位乘数")
    levels_table.add_column("人工复核")
    levels_table.add_column("默认动作")
    for level in sorted(risk_events.levels, key=lambda item: item.level):
        levels_table.add_row(
            level.level,
            level.name,
            f"{level.target_ai_exposure_multiplier:.0%}",
            "需要" if level.requires_manual_review else "不需要",
            level.default_action,
        )
    console.print(levels_table)

    rules_table = Table(title="风险事件规则")
    rules_table.add_column("事件")
    rules_table.add_column("等级")
    rules_table.add_column("活跃")
    rules_table.add_column("影响节点")
    rules_table.add_column("相关标的")
    for rule in sorted(risk_events.event_rules, key=lambda item: item.event_id):
        if active_only and not rule.active:
            continue
        rules_table.add_row(
            rule.event_id,
            rule.level,
            "是" if rule.active else "否",
            ", ".join(rule.affected_nodes),
            ", ".join(rule.related_tickers),
        )
    console.print(rules_table)


@risk_events_app.command("validate")
def validate_risk_events(
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件校验报告输出路径。"),
    ] = None,
) -> None:
    """校验风险事件等级、产业链引用和动作规则。"""
    universe = load_universe()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_events_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_risk_events_config(
        risk_events=load_risk_events(config_path),
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        universe=universe,
        as_of=validation_date,
    )
    write_risk_events_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]风险事件校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"风险事件规则数：{len(report.config.event_rules)}；活跃：{report.active_rule_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


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
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
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


@fundamentals_app.command("validate-sec-companyfacts")
def validate_sec_companyfacts_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    input_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输入目录。"),
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
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
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
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
        console.print(
            f"[{status_style}]SEC 基本面特征构建状态：{report.status}[/{status_style}]"
        )
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


@valuation_app.command("list")
def list_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
) -> None:
    """列出估值、预期和拥挤度快照。"""
    store = load_valuation_snapshot_store(input_path)

    table = Table(title="估值与拥挤度快照")
    table.add_column("Snapshot")
    table.add_column("Ticker")
    table.add_column("日期")
    table.add_column("来源")
    table.add_column("估值分位")
    table.add_column("评估")
    table.add_column("文件")

    for loaded in sorted(store.loaded, key=lambda item: item.snapshot.snapshot_id):
        snapshot = loaded.snapshot
        percentile = (
            "n/a"
            if snapshot.valuation_percentile is None
            else f"{snapshot.valuation_percentile:.0f}"
        )
        table.add_row(
            snapshot.snapshot_id,
            snapshot.ticker,
            snapshot.as_of.isoformat(),
            _valuation_source_type_label(snapshot.source_type),
            percentile,
            _valuation_assessment_label(snapshot.overall_assessment),
            str(loaded.path),
        )

    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的估值快照。")
    if store.load_errors:
        console.print(
            f"[red]存在 {len(store.load_errors)} 个加载错误，请运行 validate 查看。[/red]"
        )


@valuation_app.command("validate")
def validate_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值校验报告输出路径。"),
    ] = None,
) -> None:
    """校验估值、预期和拥挤度快照。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=validation_date,
    )
    write_valuation_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]估值快照校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"快照数量：{report.snapshot_count}；覆盖标的：{report.ticker_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("review")
def review_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值复核报告输出路径。"),
    ] = None,
) -> None:
    """复核估值、预期和拥挤度快照。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_valuation_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=review_date,
    )
    review_report = build_valuation_review_report(validation_report)
    write_valuation_review_report(review_report, report_path)

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
    )
    console.print(f"[{status_style}]估值复核状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"快照数量：{validation_report.snapshot_count}；"
        f"覆盖标的：{validation_report.ticker_count}"
    )
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)


@app.command("review-trades")
def review_trades(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    input_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trades",
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

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
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
        manifest_path=_download_manifest_path(prices_path),
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
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    sec_metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    fundamental_feature_config_path: Annotated[
        Path,
        typer.Option(help="SEC 基本面特征公式配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    sec_fundamentals_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径，用于日报基本面评分。"),
    ] = None,
    sec_fundamental_features_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面特征 CSV 输出路径，用于日报基本面评分。"),
    ] = None,
    sec_fundamental_feature_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面特征报告输出路径。"),
    ] = None,
    sec_metrics_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
    thesis_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于写入日报复核摘要。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    trades_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径，用于写入日报复盘摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "trades",
    review_benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的日报交易复盘归因基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
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
    industry_chain = load_industry_chain()
    watchlist = load_watchlist()
    data_quality_config = load_data_quality()
    feature_config = load_features()
    scoring_rules = load_scoring_rules()
    portfolio = load_portfolio()
    score_date = _parse_date(as_of) if as_of else date.today()
    benchmark_tickers = tuple(_parse_csv_items(review_benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("日报交易复盘至少需要一个归因基准标的。")
    expected_price_tickers = configured_price_tickers(
        universe,
        include_full_ai_chain=full_universe,
    )
    expected_price_tickers = list(dict.fromkeys([*expected_price_tickers, *benchmark_tickers]))
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
    sec_fundamentals_input = sec_fundamentals_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        score_date,
    )
    sec_metrics_validation_output = (
        sec_metrics_validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    sec_fundamental_features_output = (
        sec_fundamental_features_path
        or default_sec_fundamental_features_csv_path(
            PROJECT_ROOT / "data" / "processed",
            score_date,
        )
    )
    sec_fundamental_feature_report_output = (
        sec_fundamental_feature_report_path
        or default_sec_fundamental_features_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=score_date,
        manifest_path=_download_manifest_path(prices_path),
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

    prices_frame = pd.read_csv(prices_path)
    rates_frame = pd.read_csv(rates_path)
    feature_set = build_market_features(
        prices=prices_frame,
        rates=rates_frame,
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
    sec_companies = load_sec_companies(sec_companies_path)
    sec_metrics = load_fundamental_metrics(sec_metrics_path)
    sec_metrics_validation_report = validate_sec_fundamental_metrics_csv(
        companies=sec_companies,
        metrics=sec_metrics,
        input_path=sec_fundamentals_input,
        as_of=score_date,
    )
    write_sec_fundamental_metrics_validation_report(
        sec_metrics_validation_report,
        sec_metrics_validation_output,
    )
    if not sec_metrics_validation_report.passed:
        console.print("[red]SEC 基本面指标 CSV 校验失败，已停止每日评分。[/red]")
        console.print(f"SEC 指标 CSV 校验报告：{sec_metrics_validation_output}")
        console.print(f"SEC 指标 CSV：{sec_fundamentals_input}")
        console.print(
            f"错误数：{sec_metrics_validation_report.error_count}；"
            f"警告数：{sec_metrics_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    sec_fundamental_feature_report = build_sec_fundamental_features_report(
        companies=sec_companies,
        feature_config=load_fundamental_features(fundamental_feature_config_path),
        input_path=sec_fundamentals_input,
        as_of=score_date,
        validation_report=sec_metrics_validation_report,
    )
    if not sec_fundamental_feature_report.passed:
        write_sec_fundamental_features_report(
            report=sec_fundamental_feature_report,
            validation_report_path=sec_metrics_validation_output,
            output_csv_path=sec_fundamental_features_output,
            output_path=sec_fundamental_feature_report_output,
        )
        console.print("[red]SEC 基本面特征构建失败，已停止每日评分。[/red]")
        console.print(f"SEC 基本面特征报告：{sec_fundamental_feature_report_output}")
        console.print(f"SEC 指标 CSV 校验报告：{sec_metrics_validation_output}")
        console.print(
            f"错误数：{sec_fundamental_feature_report.error_count}；"
            f"警告数：{sec_fundamental_feature_report.warning_count}"
        )
        raise typer.Exit(code=1)

    sec_fundamental_features_output = write_sec_fundamental_features_csv(
        sec_fundamental_feature_report,
        sec_fundamental_features_output,
    )
    sec_fundamental_feature_report_output = write_sec_fundamental_features_report(
        report=sec_fundamental_feature_report,
        validation_report_path=sec_metrics_validation_output,
        output_csv_path=sec_fundamental_features_output,
        output_path=sec_fundamental_feature_report_output,
    )
    review_summary = _build_daily_review_summary(
        thesis_path=thesis_path,
        risk_events_path=risk_events_path,
        valuation_path=valuation_path,
        trades_path=trades_path,
        score_date=score_date,
        universe=universe,
        industry_chain=industry_chain,
        watchlist=watchlist,
        prices=prices_frame,
        data_quality_report=data_quality_report,
        benchmark_tickers=benchmark_tickers,
    )
    score_report = build_daily_score_report(
        feature_set=feature_set,
        data_quality_report=data_quality_report,
        rules=scoring_rules,
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
        review_summary=review_summary,
        fundamental_feature_report=sec_fundamental_feature_report,
    )
    scores_output = write_scores_csv(score_report, scores_path)
    daily_report_output = write_daily_score_report(
        score_report,
        data_quality_report_path=quality_output,
        feature_report_path=feature_summary_output,
        features_path=features_output,
        scores_path=scores_output,
        output_path=score_report_output,
        sec_metrics_validation_report_path=sec_metrics_validation_output,
        sec_fundamental_feature_report_path=sec_fundamental_feature_report_output,
        sec_fundamental_features_path=sec_fundamental_features_output,
    )

    status_style = "green" if score_report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]每日评分状态：{score_report.status}[/{status_style}]")
    console.print(f"总分：{score_report.recommendation.total_score:.1f}")
    console.print(f"仓位状态：{score_report.recommendation.label}")
    console.print(f"每日评分报告：{daily_report_output}")
    console.print(f"评分数据：{scores_output}")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(
        f"SEC 基本面特征：{sec_fundamental_features_output}"
        f"（{sec_fundamental_feature_report.status}）"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


def _build_daily_review_summary(
    thesis_path: Path,
    risk_events_path: Path,
    valuation_path: Path,
    trades_path: Path,
    score_date: date,
    universe: UniverseConfig,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    prices: pd.DataFrame,
    data_quality_report: DataQualityReport,
    benchmark_tickers: tuple[str, ...],
) -> DailyReviewSummary:
    return DailyReviewSummary(
        thesis=_build_daily_thesis_status(
            input_path=thesis_path,
            watchlist=watchlist,
            industry_chain=industry_chain,
            score_date=score_date,
        ),
        risk_events=_build_daily_risk_events_status(
            input_path=risk_events_path,
            universe=universe,
            industry_chain=industry_chain,
            watchlist=watchlist,
            score_date=score_date,
        ),
        valuation=_build_daily_valuation_status(
            input_path=valuation_path,
            universe=universe,
            watchlist=watchlist,
            score_date=score_date,
        ),
        trades=_build_daily_trade_review_status(
            input_path=trades_path,
            universe=universe,
            watchlist=watchlist,
            score_date=score_date,
            prices=prices,
            data_quality_report=data_quality_report,
            benchmark_tickers=benchmark_tickers,
        ),
    )


def _build_daily_thesis_status(
    input_path: Path,
    watchlist: WatchlistConfig,
    industry_chain: IndustryChainConfig,
    score_date: date,
) -> DailyManualReviewStatus:
    validation_report = validate_trade_thesis_store(
        store=load_trade_thesis_store(input_path),
        watchlist=watchlist,
        industry_chain=industry_chain,
        as_of=score_date,
    )
    review_report = build_thesis_review_report(validation_report)

    watch_count = sum(item.health == "WATCH" for item in review_report.items)
    invalidated_count = sum(item.health == "INVALIDATED" for item in review_report.items)
    summary = (
        f"Thesis {validation_report.thesis_count} 个，活跃 "
        f"{validation_report.active_count} 个；需关注 {watch_count} 个，"
        f"已证伪 {invalidated_count} 个。"
    )
    return DailyManualReviewStatus(
        name="交易 thesis",
        status=review_report.status,
        summary=summary,
        error_count=validation_report.error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _build_daily_risk_events_status(
    input_path: Path,
    universe: UniverseConfig,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    score_date: date,
) -> DailyManualReviewStatus:
    try:
        validation_report = validate_risk_events_config(
            risk_events=load_risk_events(input_path),
            industry_chain=industry_chain,
            watchlist=watchlist,
            universe=universe,
            as_of=score_date,
        )
    except (OSError, ValueError, YAMLError) as exc:
        return _daily_review_exception_status("风险事件", input_path, exc)

    active_l2_l3_count = sum(
        1
        for rule in validation_report.config.event_rules
        if rule.active and rule.level in {"L2", "L3"}
    )
    summary = (
        f"风险规则 {len(validation_report.config.event_rules)} 条，活跃 "
        f"{validation_report.active_rule_count} 条；活跃 L2/L3 规则 "
        f"{active_l2_l3_count} 条。"
    )
    return DailyManualReviewStatus(
        name="风险事件",
        status=validation_report.status,
        summary=summary,
        error_count=validation_report.error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _build_daily_valuation_status(
    input_path: Path,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
    score_date: date,
) -> DailyManualReviewStatus:
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=universe,
        watchlist=watchlist,
        as_of=score_date,
    )
    review_report = build_valuation_review_report(validation_report)

    overheated_count = sum(
        item.health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}
        for item in review_report.items
    )
    summary = (
        f"估值快照 {validation_report.snapshot_count} 个，覆盖 "
        f"{validation_report.ticker_count} 个标的；偏贵或拥挤 "
        f"{overheated_count} 个。"
    )
    return DailyManualReviewStatus(
        name="估值与拥挤度",
        status=review_report.status,
        summary=summary,
        error_count=validation_report.error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _build_daily_trade_review_status(
    input_path: Path,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
    score_date: date,
    prices: pd.DataFrame,
    data_quality_report: DataQualityReport,
    benchmark_tickers: tuple[str, ...],
) -> DailyManualReviewStatus:
    try:
        validation_report = validate_trade_record_store(
            store=load_trade_record_store(input_path),
            universe=universe,
            watchlist=watchlist,
            as_of=score_date,
        )
        review_report = build_trade_review_report(
            validation_report=validation_report,
            prices=prices,
            data_quality_report=data_quality_report,
            benchmark_tickers=benchmark_tickers,
        )
    except ValueError as exc:
        return _daily_review_exception_status("交易复盘归因", input_path, exc)

    summary = (
        f"交易记录 {validation_report.trade_count} 笔，已关闭 "
        f"{validation_report.closed_count} 笔；生成基准归因 "
        f"{len(review_report.items)} 条。"
    )
    return DailyManualReviewStatus(
        name="交易复盘归因",
        status=review_report.status,
        summary=summary,
        error_count=validation_report.error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _daily_review_exception_status(
    name: str,
    source_path: Path,
    exc: Exception,
) -> DailyManualReviewStatus:
    return DailyManualReviewStatus(
        name=name,
        status="FAIL",
        summary=f"模块加载或复核失败：{exc}",
        error_count=1,
        warning_count=0,
        source_path=source_path,
    )


def _download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


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


def _thesis_direction_label(value: str) -> str:
    return {
        "long": "做多",
        "short": "做空",
        "hedge": "对冲",
        "watch": "观察",
    }.get(value, value)


def _thesis_status_label(value: str) -> str:
    return {
        "draft": "草稿",
        "active": "活跃",
        "paused": "暂停",
        "closed": "已关闭",
        "invalidated": "已证伪",
    }.get(value, value)


def _thesis_review_frequency_label(value: str) -> str:
    return {
        "daily": "每日",
        "weekly": "每周",
        "monthly": "每月",
        "quarterly": "每季",
        "event_driven": "事件驱动",
    }.get(value, value)


def _valuation_source_type_label(value: str) -> str:
    return {
        "primary_filing": "一手披露",
        "paid_vendor": "付费供应商",
        "manual_input": "手工录入",
        "public_convenience": "公开便利源",
    }.get(value, value)


def _valuation_assessment_label(value: str) -> str:
    return {
        "cheap": "偏便宜",
        "reasonable": "合理",
        "expensive": "偏贵",
        "extreme": "极端",
        "unknown": "未知",
    }.get(value, value)


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


if __name__ == "__main__":
    app()
