from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.allocation import load_allocation
from ai_trading_system.etf_portfolio.data import (
    load_standard_prices,
    read_price_frame,
    standardize_price_frame,
    validate_price_data,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_BACKTEST_DIR,
    DEFAULT_ETF_P2_MANIFEST_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_SIGNAL_PATH,
    DEFAULT_ETF_TARGET_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.p1 import write_frame_and_report
from ai_trading_system.etf_portfolio.p2 import (
    build_advanced_risk_report,
    build_edgar_text_topic_audit,
    build_ensemble_candidates,
    build_holdings_lookthrough_report,
    build_live_interface_preflight,
    build_ml_ranking_candidates,
    build_news_theme_tracking_report,
    build_source_contract_report,
    build_walk_forward_readiness_report,
    build_weight_optimizer_candidates,
    derive_edgar_text_events_from_timeline,
    derive_options_iv_skew_from_vix,
    fetch_edgar_text_documents_from_timeline,
    import_p2_source,
    normalize_etf_holdings_source,
    normalize_news_theme_source,
    normalize_options_risk_source,
    p2_metadata,
)
from ai_trading_system.etf_portfolio.signals import load_signals
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    quality_metadata as _quality_metadata,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    resolve_frame_date as _resolve_frame_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import p2_app


@p2_app.command("edgar-text")
def p2_edgar_text_command(
    input_path: Annotated[Path | None, typer.Option(help="EDGAR/text audit input CSV。")] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 EDGAR / 财报文本解释层 contract report；observe-only。"""
    _write_p2_source_contract(
        source_id="edgar_text",
        title="ETF P2 EDGAR Text Contract",
        input_path=input_path,
        date_option=date_option,
        output_dir=output_dir,
    )


@p2_app.command("derive-edgar-events")
def p2_derive_edgar_events_command(
    timeline_path: Annotated[
        Path,
        typer.Option(help="SEC PIT filing timeline CSV/Parquet。"),
    ] = PROJECT_ROOT / "data" / "processed" / "sec_edgar" / "filing_timeline.csv",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical edgar_text_events 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", help="只派生指定 ticker，可重复。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="派生报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """从本地 SEC PIT filing timeline 派生 P2 EDGAR metadata events；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["edgar_text"]
    run_date = _resolve_p2_source_date(date_option, timeline_path, "available_for_signal_date")
    frame = derive_edgar_text_events_from_timeline(
        source=source,
        timeline_path=timeline_path,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=run_date,
        symbols=symbols,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "derive_edgar_events",
        "ETF P2 Derive EDGAR Events",
        config,
    )


@p2_app.command("fetch-edgar-text")
def p2_fetch_edgar_text_command(
    timeline_path: Annotated[
        Path,
        typer.Option(help="SEC PIT filing timeline CSV/Parquet。"),
    ] = PROJECT_ROOT / "data" / "processed" / "sec_edgar" / "filing_timeline.csv",
    document_dir: Annotated[
        Path,
        typer.Option(help="SEC filing 文本缓存目录。"),
    ] = PROJECT_ROOT / "data" / "etf_portfolio" / "p2" / "edgar_text_documents",
    output_path: Annotated[
        Path,
        typer.Option(help="EDGAR text document index 输出路径。"),
    ] = PROJECT_ROOT / "data" / "etf_portfolio" / "p2" / "edgar_text_documents.csv",
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", help="只获取指定 ticker，可重复。"),
    ] = None,
    filing_types: Annotated[
        list[str] | None,
        typer.Option("--filing-type", help="只获取指定 SEC form，可重复，例如 10-K/10-Q/8-K。"),
    ] = None,
    limit: Annotated[int, typer.Option(help="本次最多获取的 filing 数。", min=1)] = 5,
    user_agent: Annotated[
        str | None,
        typer.Option(help="SEC User-Agent；不填则读取 SEC_USER_AGENT。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="文本获取报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """从 SEC PIT filing timeline 获取有限 official filing 文本；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["edgar_text"]
    run_date = _resolve_p2_source_date(date_option, timeline_path, "available_for_signal_date")
    frame = fetch_edgar_text_documents_from_timeline(
        source=source,
        timeline_path=timeline_path,
        document_dir=document_dir,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=run_date,
        symbols=symbols,
        filing_types=filing_types,
        limit=limit,
        user_agent=user_agent,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "fetch_edgar_text",
        "ETF P2 Fetch EDGAR Text Documents",
        config,
    )


@p2_app.command("edgar-topics")
def p2_edgar_topics_command(
    input_path: Annotated[
        Path,
        typer.Option(help="EDGAR text document index CSV。"),
    ] = PROJECT_ROOT / "data" / "etf_portfolio" / "p2" / "edgar_text_documents.csv",
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="EDGAR topic audit 输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """对已缓存 EDGAR filing text 做受治理关键词 topic audit；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    document_index = pd.read_csv(input_path)
    run_date = _resolve_p2_source_date(date_option, input_path, "as_of")
    frame = build_edgar_text_topic_audit(
        document_index=document_index,
        p2_config=p2_config,
        run_date=run_date,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "edgar_topics",
        "ETF P2 EDGAR Text Topic Audit",
        config,
    )


@p2_app.command("import-source")
def p2_import_source_command(
    source_id: Annotated[
        str,
        typer.Argument(help="P2 source id: edgar_text/news_themes/options_iv_skew/etf_holdings。"),
    ],
    input_path: Annotated[Path, typer.Option(help="待导入的审计 CSV/Parquet。")],
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    provider: Annotated[str, typer.Option(help="Provider name。")] = "manual_input",
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = "manual_input",
    output_dir: Annotated[Path, typer.Option(help="导入报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """导入 P2 本地审计输入，写 canonical CSV 和 source manifest。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    if source_id not in p2_config.sources:
        raise typer.BadParameter(f"未知 P2 source_id：{source_id}")
    source = p2_config.sources[source_id]
    frame = import_p2_source(
        source_id=source_id,
        source=source,
        input_path=input_path,
        output_path=output_path,
        manifest_path=manifest_path,
        provider=provider,
        source_url=source_url,
        request_params={"input_path": str(input_path)},
    )
    run_date = date.today()
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        f"import_{source_id}",
        f"ETF P2 Import {source_id}",
        config,
    )


@p2_app.command("normalize-holdings")
def p2_normalize_holdings_command(
    input_path: Annotated[Path, typer.Option(help="Issuer/vendor/manual holdings CSV/Parquet。")],
    etf_symbol: Annotated[str, typer.Option(help="ETF symbol，例如 SMH、SOXX、QQQ。")],
    date_option: Annotated[str, typer.Option("--date", help="Holdings as-of 日期 YYYY-MM-DD。")],
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical holdings 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    provider: Annotated[str, typer.Option(help="Provider / issuer name。")] = "manual_input",
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = "manual_input",
    downloaded_at: Annotated[
        str | None,
        typer.Option(help="下载或接收时间 ISO-8601；默认当前 UTC。"),
    ] = None,
    holding_symbol_column: Annotated[
        str | None,
        typer.Option(help="持仓 symbol 列名；不填则识别常见列名。"),
    ] = None,
    holding_weight_column: Annotated[
        str | None,
        typer.Option(help="持仓权重列名；不填则识别常见列名。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="规范化报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """规范化 ETF holdings 输入为 canonical holdings；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["etf_holdings"]
    run_date = _parse_date(date_option)
    frame = normalize_etf_holdings_source(
        source=source,
        input_path=input_path,
        etf_symbol=etf_symbol,
        provider=provider,
        source_url=source_url,
        as_of=run_date,
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=_parse_datetime(downloaded_at) if downloaded_at else None,
        holding_symbol_column=holding_symbol_column,
        holding_weight_column=holding_weight_column,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        f"normalize_holdings_{etf_symbol.upper()}",
        f"ETF P2 Normalize Holdings {etf_symbol.upper()}",
        config,
    )


@p2_app.command("news-themes")
def p2_news_themes_command(
    input_path: Annotated[Path | None, typer.Option(help="News theme audit input CSV。")] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 新闻摘要和主题事件追踪 report；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["news_themes"]
    path = input_path or _p2_input_path(source.input_path)
    run_date = _resolve_p2_source_date(date_option, path, source.as_of_column)
    frame = build_news_theme_tracking_report(
        source=source,
        p2_config=p2_config,
        run_date=run_date,
        input_path=path,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "news_themes",
        "ETF P2 News Theme Tracking",
        config,
    )


@p2_app.command("normalize-news")
def p2_normalize_news_command(
    input_path: Annotated[Path, typer.Option(help="Vendor/manual news theme CSV/Parquet。")],
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical news_theme_events 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    provider: Annotated[str, typer.Option(help="Provider / audit source name。")] = (
        "manual_input"
    ),
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = ("manual_input"),
    downloaded_at: Annotated[
        str | None,
        typer.Option(help="下载或接收时间 ISO-8601；默认当前 UTC。"),
    ] = None,
    symbol_column: Annotated[str | None, typer.Option(help="symbol 列名。")] = None,
    theme_column: Annotated[str | None, typer.Option(help="theme/topic 列名。")] = None,
    summary_column: Annotated[str | None, typer.Option(help="summary/headline 列名。")] = None,
    published_at_column: Annotated[str | None, typer.Option(help="published_at 列名。")] = None,
    available_at_column: Annotated[str | None, typer.Option(help="available_at 列名。")] = None,
    sentiment_column: Annotated[str | None, typer.Option(help="sentiment_score 列名。")] = None,
    relevance_column: Annotated[str | None, typer.Option(help="relevance_score 列名。")] = None,
    output_dir: Annotated[Path, typer.Option(help="规范化报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """规范化 P2 news/theme 输入；observe-only，不调用 LLM。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["news_themes"]
    frame = normalize_news_theme_source(
        source=source,
        p2_config=p2_config,
        input_path=input_path,
        provider=provider,
        source_url=source_url,
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=_parse_datetime(downloaded_at) if downloaded_at else None,
        symbol_column=symbol_column,
        theme_column=theme_column,
        summary_column=summary_column,
        published_at_column=published_at_column,
        available_at_column=available_at_column,
        sentiment_column=sentiment_column,
        relevance_column=relevance_column,
    )
    run_date = _resolve_frame_date("latest", frame, "downloaded_at")
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "normalize_news_themes",
        "ETF P2 Normalize News Themes",
        config,
    )


@p2_app.command("options-risk")
def p2_options_risk_command(
    input_path: Annotated[
        Path | None,
        typer.Option(help="Options IV/skew audit input CSV。"),
    ] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 期权 IV / VXN / skew 过滤 contract report；observe-only。"""
    _write_p2_source_contract(
        source_id="options_iv_skew",
        title="ETF P2 Options IV Skew Contract",
        input_path=input_path,
        date_option=date_option,
        output_dir=output_dir,
    )


@p2_app.command("normalize-options-risk")
def p2_normalize_options_risk_command(
    input_path: Annotated[Path, typer.Option(help="Vendor/manual options IV/VXN/skew CSV。")],
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical options_iv_skew 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    provider: Annotated[str, typer.Option(help="Provider / audit source name。")] = (
        "manual_input"
    ),
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = ("manual_input"),
    downloaded_at: Annotated[
        str | None,
        typer.Option(help="下载或接收时间 ISO-8601；默认当前 UTC。"),
    ] = None,
    symbol_column: Annotated[str | None, typer.Option(help="symbol/ticker 列名。")] = None,
    as_of_column: Annotated[str | None, typer.Option(help="as_of/date 列名。")] = None,
    available_at_column: Annotated[str | None, typer.Option(help="available_at 列名。")] = None,
    iv_rank_column: Annotated[str | None, typer.Option(help="iv_rank 列名。")] = None,
    skew_zscore_column: Annotated[str | None, typer.Option(help="skew_zscore 列名。")] = None,
    vxn_level_column: Annotated[str | None, typer.Option(help="vxn_level 列名。")] = None,
    risk_flag_column: Annotated[str | None, typer.Option(help="可选 risk_flag 列名。")] = None,
    output_dir: Annotated[Path, typer.Option(help="规范化报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """规范化 P2 options IV/VXN/skew 输入；observe-only，不下载 provider。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["options_iv_skew"]
    frame = normalize_options_risk_source(
        source=source,
        p2_config=p2_config,
        input_path=input_path,
        provider=provider,
        source_url=source_url,
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=_parse_datetime(downloaded_at) if downloaded_at else None,
        symbol_column=symbol_column,
        as_of_column=as_of_column,
        available_at_column=available_at_column,
        iv_rank_column=iv_rank_column,
        skew_zscore_column=skew_zscore_column,
        vxn_level_column=vxn_level_column,
        risk_flag_column=risk_flag_column,
    )
    run_date = _resolve_frame_date("latest", frame, "downloaded_at")
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "normalize_options_risk",
        "ETF P2 Normalize Options Risk",
        config,
    )


@p2_app.command("derive-options-risk")
def p2_derive_options_risk_command(
    prices_path: Annotated[Path, typer.Option(help="含 ^VIX 的市场价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical options_iv_skew 输出路径，默认读取 p2.yaml。"),
    ] = None,
    manifest_path: Annotated[Path, typer.Option(help="P2 source manifest 输出路径。")] = (
        DEFAULT_ETF_P2_MANIFEST_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", help="输出目标 ETF symbol，可重复；默认全部 tradeable ETF。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="派生报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "p2"
    ),
) -> None:
    """从本地 ^VIX 市场缓存派生 P2 options risk proxy；显式保留 VXN/skew 缺口。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["options_iv_skew"]
    run_date = _resolve_date(date_option, prices_path=prices_path)
    raw = read_price_frame(prices_path)
    prices, metadata_issues = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name=str(prices_path),
    )
    quality_report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=run_date,
        extra_issues=metadata_issues,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 P2 options-risk 派生。")
        raise typer.Exit(code=1)
    target_symbols = symbols or list(config.assets.tradeable_symbols)
    frame = derive_options_iv_skew_from_vix(
        source=source,
        p2_config=p2_config,
        prices=raw,
        prices_path=prices_path,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=run_date,
        symbols=target_symbols,
        data_quality_status=quality_report.status,
    )
    metadata = {**p2_metadata(config), **_quality_metadata(quality_report)}
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "derive_options_risk",
        "ETF P2 Derive Options Risk",
        config,
        metadata,
    )


@p2_app.command("holdings-lookthrough")
def p2_holdings_lookthrough_command(
    input_path: Annotated[Path | None, typer.Option(help="ETF holdings audit input CSV。")] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 ETF 持仓穿透 contract/look-through report；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources["etf_holdings"]
    path = input_path or _p2_input_path(source.input_path)
    run_date = _resolve_p2_source_date(date_option, path, source.as_of_column)
    frame = build_holdings_lookthrough_report(
        source=source,
        run_date=run_date,
        input_path=path,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "holdings_lookthrough",
        "ETF P2 Holdings Lookthrough",
        config,
    )


@p2_app.command("advanced-risk")
def p2_advanced_risk_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 高级风险模型基础诊断；observe-only。"""
    config = load_etf_config_bundle()
    _require_p2_config(config)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 P2 advanced-risk。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    allocation = load_allocation(allocation_path)
    frame = build_advanced_risk_report(
        allocation=allocation,
        prices=prices,
        config=config,
        quality_report=quality_report,
        run_date=run_date,
    )
    metadata = {**p2_metadata(config), **_quality_metadata(quality_report)}
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "advanced_risk",
        "ETF P2 Advanced Risk",
        config,
        metadata,
    )


@p2_app.command("walk-forward")
def p2_walk_forward_command(
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    backtest_dir: Annotated[Path, typer.Option(help="ETF backtest 输出根目录。")] = (
        DEFAULT_ETF_BACKTEST_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 walk-forward readiness report；observe-only。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    frame = build_walk_forward_readiness_report(
        backtest_dir=backtest_dir,
        p2_config=p2_config,
        run_date=run_date,
    )
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "walk_forward",
        "ETF P2 Walk Forward Readiness",
        config,
    )


@p2_app.command("ml-ranking")
def p2_ml_ranking_command(
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 ML ranking candidate report；不进入 production。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    signals = load_signals(signals_path)
    run_date = _resolve_frame_date(date_option, signals)
    frame = build_ml_ranking_candidates(signals, p2_config=p2_config, run_date=run_date)
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "ml_ranking",
        "ETF P2 ML Ranking Candidates",
        config,
    )


@p2_app.command("weight-optimizer")
def p2_weight_optimizer_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 candidate-only weight optimizer report；不写正式目标权重。"""
    config = load_etf_config_bundle()
    _require_p2_config(config)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 P2 weight-optimizer。")
        raise typer.Exit(code=1)
    signals = load_signals(signals_path)
    run_date = _resolve_frame_date(date_option, signals)
    frame = build_weight_optimizer_candidates(
        signals=signals,
        prices=prices,
        config=config,
        quality_report=quality_report,
        run_date=run_date,
    )
    metadata = {**p2_metadata(config), **_quality_metadata(quality_report)}
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "weight_optimizer",
        "ETF P2 Weight Optimizer Candidates",
        config,
        metadata,
    )


@p2_app.command("ensemble")
def p2_ensemble_command(
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 ensemble candidate report；不进入 production。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    signals = load_signals(signals_path)
    run_date = _resolve_frame_date(date_option, signals)
    ml = build_ml_ranking_candidates(signals, p2_config=p2_config, run_date=run_date)
    frame = build_ensemble_candidates(signals, ml, p2_config=p2_config, run_date=run_date)
    _write_p2_frame(frame, output_dir, run_date, "ensemble", "ETF P2 Ensemble Candidates", config)


@p2_app.command("live-preflight")
def p2_live_preflight_command(
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p2",
) -> None:
    """生成 P2 多账户/实盘接口 preflight；默认阻断 broker route。"""
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    frame = build_live_interface_preflight(p2_config=p2_config, run_date=run_date)
    _write_p2_frame(
        frame,
        output_dir,
        run_date,
        "live_preflight",
        "ETF P2 Live Interface Preflight",
        config,
    )


def _resolve_p2_source_date(value: str | None, path: Path, as_of_column: str) -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    if not path.exists():
        return date.today()
    frame = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)
    if as_of_column not in frame.columns:
        return date.today()
    return _resolve_frame_date("latest", frame, as_of_column)


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = pd.Timestamp(value).to_pydatetime()
    except ValueError as exc:
        raise typer.BadParameter("时间必须使用 ISO-8601 格式") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _require_p2_config(config):
    if config.p2 is None:
        raise typer.BadParameter("缺少 ETF P2 config")
    return config.p2


def _p2_input_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _write_p2_source_contract(
    *,
    source_id: str,
    title: str,
    input_path: Path | None,
    date_option: str | None,
    output_dir: Path,
) -> None:
    config = load_etf_config_bundle()
    p2_config = _require_p2_config(config)
    source = p2_config.sources[source_id]
    path = input_path or _p2_input_path(source.input_path)
    run_date = _resolve_p2_source_date(date_option, path, source.as_of_column)
    frame = build_source_contract_report(
        source_id=source_id,
        source=source,
        run_date=run_date,
        input_path=path,
    )
    _write_p2_frame(frame, output_dir, run_date, source_id, title, config)


def _write_p2_frame(
    frame: pd.DataFrame,
    output_dir: Path,
    run_date: date,
    stem: str,
    title: str,
    config,
    metadata: dict[str, object] | None = None,
) -> None:
    csv_path = output_dir / f"{run_date.isoformat()}_{stem}.csv"
    md_path = output_dir / f"{run_date.isoformat()}_{stem}.md"
    write_frame_and_report(
        frame,
        csv_path,
        md_path,
        title,
        metadata=metadata or p2_metadata(config),
    )
    typer.echo(f"{title}：{md_path}")
