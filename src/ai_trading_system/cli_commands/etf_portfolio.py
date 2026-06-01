from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.ai_confirmation import (
    DEFAULT_AI_CONFIRMATION_FEATURE_DIR,
    DEFAULT_AI_CONFIRMATION_OVERLAY_DIR,
    DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH,
    DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH,
    ai_confirmation_price_group_ids,
    all_enabled_price_tickers,
    build_ai_confirmation_breadth_features,
    build_ai_confirmation_report,
    build_ai_confirmation_shadow_overlay_experiment,
    latest_ai_confirmation_report_path,
    load_ai_confirmation_base_weights,
    load_ai_confirmation_events,
    load_ai_confirmation_policy_config,
    load_ai_confirmation_universe_config,
    validate_ai_confirmation_data_availability,
    write_ai_confirmation_breadth_features,
    write_ai_confirmation_report,
    write_ai_confirmation_shadow_overlay,
)
from ai_trading_system.etf_portfolio.allocation import (
    allocate_portfolio,
    latest_weights_from_file,
    load_allocation,
    write_allocation,
)
from ai_trading_system.etf_portfolio.backtest import run_portfolio_backtest, write_backtest_run
from ai_trading_system.etf_portfolio.credibility import (
    DEFAULT_ETF_CREDIBILITY_DIR,
    build_credibility_gate,
    write_credibility_gate,
)
from ai_trading_system.etf_portfolio.data import (
    ingest_yfinance_prices,
    latest_price_date,
    load_standard_prices,
    read_price_frame,
    standardize_price_frame,
    validate_price_data,
    write_quality_report,
)
from ai_trading_system.etf_portfolio.experiments import (
    DEFAULT_ETF_EXPERIMENT_RUN_DIR,
    DEFAULT_ETF_EXPERIMENT_WEEKLY_REVIEW_DIR,
    DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    apply_ranking_policy_to_comparison_report,
    build_candidate_selection_report,
    build_experiment_comparison_report,
    build_experiment_validation_report,
    build_weekly_experiment_review,
    enroll_shadow_candidates,
    find_latest_experiment_run_dir,
    load_experiment_pack_registry,
    load_experiment_registry,
    run_experiment_batch,
    write_candidate_selection_report,
    write_experiment_comparison_report,
    write_experiment_validation_report,
    write_weekly_experiment_review_report,
)
from ai_trading_system.etf_portfolio.features import (
    build_feature_store,
    latest_feature_date,
    load_feature_store,
    write_feature_store,
)
from ai_trading_system.etf_portfolio.forward import (
    DEFAULT_ETF_FORWARD_CONFIG_PATH,
    DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    DEFAULT_ETF_FORWARD_REPORT_DIR,
    run_forward_dashboard,
    run_forward_update,
    run_forward_validation,
    run_forward_watchlist,
    run_forward_weekly_review,
)
from ai_trading_system.etf_portfolio.governance import (
    DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH,
    evaluate_parameter_candidate,
    load_parameter_governance_policy,
    read_parameter_candidate,
    write_parameter_governance_summary,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    DEFAULT_ETF_BACKTEST_DIR,
    DEFAULT_ETF_FEATURE_PATH,
    DEFAULT_ETF_LEDGER_PATH,
    DEFAULT_ETF_P2_MANIFEST_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REGIME_PATH,
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_SIGNAL_PATH,
    DEFAULT_ETF_STRATEGY_CONFIG_PATH,
    DEFAULT_ETF_TARGET_PATH,
    ETFAllocationRecord,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.p1 import (
    append_experiment_registry,
    append_experiment_run,
    build_confirmation_scores,
    build_experiment_comparison,
    build_governance_status,
    build_portfolio_attribution,
    build_relative_strength_table,
    evaluate_event_risk,
    evaluate_satellite_candidates,
    write_frame_and_report,
)
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
from ai_trading_system.etf_portfolio.regime import (
    generate_regime_for_date,
    load_regimes,
    select_regime_for_date,
    write_regime,
)
from ai_trading_system.etf_portfolio.reporting import render_daily_brief, write_daily_brief
from ai_trading_system.etf_portfolio.signals import (
    generate_signals_for_date,
    load_signals,
    select_signals_for_date,
    signals_to_frame,
    write_signals,
)
from ai_trading_system.etf_portfolio.simulation import (
    evaluate_simulation_ledger,
    record_simulation_snapshot,
    summarize_simulation_for_brief,
    write_simulation_report,
)
from ai_trading_system.etf_portfolio.stability import (
    build_allocation_stability_diagnostics,
    write_allocation_stability_diagnostics,
)

etf_app = typer.Typer(help="ETF 主仓组合配置、信号、回测和模拟舱。", no_args_is_help=True)
data_app = typer.Typer(help="ETF price ingest and validation。", no_args_is_help=True)
features_app = typer.Typer(help="ETF feature store。", no_args_is_help=True)
signals_app = typer.Typer(help="ETF signal engine。", no_args_is_help=True)
regime_app = typer.Typer(help="ETF market regime engine。", no_args_is_help=True)
portfolio_app = typer.Typer(help="ETF portfolio allocation。", no_args_is_help=True)
backtest_app = typer.Typer(help="ETF portfolio backtest。", no_args_is_help=True)
simulation_app = typer.Typer(help="ETF simulation ledger。", no_args_is_help=True)
report_app = typer.Typer(help="ETF portfolio reports。", no_args_is_help=True)
run_app = typer.Typer(help="ETF portfolio full workflow。", no_args_is_help=True)
relative_strength_app = typer.Typer(help="ETF P1 relative strength。", no_args_is_help=True)
confirmation_app = typer.Typer(help="ETF P1 confirmation scores。", no_args_is_help=True)
satellite_app = typer.Typer(help="ETF P1 satellite candidates。", no_args_is_help=True)
attribution_app = typer.Typer(help="ETF P1 attribution。", no_args_is_help=True)
experiments_app = typer.Typer(help="ETF P1 experiment registry。", no_args_is_help=True)
forward_app = typer.Typer(help="ETF forward shadow simulation review。", no_args_is_help=True)
ai_confirmation_app = typer.Typer(
    help="ETF AI confirmation overlay calibration。",
    no_args_is_help=True,
)
governance_app = typer.Typer(help="ETF P1 weight governance。", no_args_is_help=True)
events_app = typer.Typer(help="ETF P1 event risk flags。", no_args_is_help=True)
p2_app = typer.Typer(help="ETF P2 observe-only contracts。", no_args_is_help=True)
credibility_app = typer.Typer(help="ETF credibility validation gate。", no_args_is_help=True)

etf_app.add_typer(data_app, name="data")
etf_app.add_typer(features_app, name="features")
etf_app.add_typer(signals_app, name="signals")
etf_app.add_typer(regime_app, name="regime")
etf_app.add_typer(portfolio_app, name="portfolio")
etf_app.add_typer(backtest_app, name="backtest")
etf_app.add_typer(simulation_app, name="simulation")
etf_app.add_typer(report_app, name="report")
etf_app.add_typer(run_app, name="run")
etf_app.add_typer(relative_strength_app, name="relative-strength")
etf_app.add_typer(confirmation_app, name="confirmation")
etf_app.add_typer(satellite_app, name="satellite")
etf_app.add_typer(attribution_app, name="attribution")
etf_app.add_typer(experiments_app, name="experiments")
etf_app.add_typer(forward_app, name="forward")
etf_app.add_typer(ai_confirmation_app, name="ai-confirmation")
etf_app.add_typer(governance_app, name="governance")
etf_app.add_typer(events_app, name="events")
etf_app.add_typer(p2_app, name="p2")
etf_app.add_typer(credibility_app, name="credibility")


@etf_app.command("validate-config")
def validate_config_command() -> None:
    """校验 ETF P0 配置。"""
    config = load_etf_config_bundle()
    typer.echo("ETF 配置校验通过。")
    typer.echo(f"model_version={config.strategy.model.version}")
    typer.echo(f"config_hash={config.config_hash}")
    typer.echo(f"assets={', '.join(config.assets.assets)}")


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
        start=_parse_date(start),
        end=_parse_date(end) if end else date.today(),
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
    run_date = _resolve_date(as_of, prices_path=prices_path)
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
        extra_symbols=_satellite_symbols(config) if include_satellites else None,
    )
    if not report.passed:
        typer.echo(f"ETF 数据质量状态：{report.status}，已停止 feature build。")
        raise typer.Exit(code=1)
    features = build_feature_store(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        start=_parse_date(start) if start else None,
        end=_resolve_date(end, prices=prices) if end else None,
    )
    write_feature_store(features, output_path)
    typer.echo(f"ETF feature store 已写入：{output_path}（{len(features)} 行）")
    typer.echo(f"data_quality_status={report.status}")


@relative_strength_app.command("report")
def relative_strength_report_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 relative strength table/report。"""
    config = load_etf_config_bundle()
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    table = build_relative_strength_table(features, config=config, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_relative_strength.csv"
    md_path = output_dir / f"{run_date.isoformat()}_relative_strength.md"
    write_frame_and_report(
        table,
        csv_path,
        md_path,
        "ETF Relative Strength Report",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF relative strength report：{md_path}")


@confirmation_app.command("report")
def confirmation_report_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 AI / semiconductor confirmation scores。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    rs_table = build_relative_strength_table(features, config=config, run_date=run_date)
    scores = build_confirmation_scores(rs_table, p1_config=config.p1, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_confirmation_scores.csv"
    md_path = output_dir / f"{run_date.isoformat()}_confirmation_scores.md"
    write_frame_and_report(
        scores,
        csv_path,
        md_path,
        "ETF Confirmation Scores",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF confirmation scores：{md_path}")


@ai_confirmation_app.command("features")
def ai_confirmation_features_command(
    prices_path: Annotated[Path, typer.Option(help="ETF / AI 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_FEATURE_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="AI confirmation universe config。")] = (
        DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-066B AI / semiconductor breadth features。"""
    config = load_etf_config_bundle()
    ai_config = load_ai_confirmation_universe_config(universe_path)
    extra_symbols = set(all_enabled_price_tickers(ai_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI confirmation features。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_ai_confirmation_data_availability(
        ai_config,
        _available_price_symbols(prices, run_date),
        group_ids=ai_confirmation_price_group_ids(ai_config),
    )
    if availability["status"] == "FAIL":
        typer.echo("AI confirmation 数据覆盖状态：FAIL，已停止 feature build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    records = build_ai_confirmation_breadth_features(
        prices,
        config=ai_config,
        run_date=run_date,
    )
    paths = write_ai_confirmation_breadth_features(
        records,
        output_dir=output_dir,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
    )
    typer.echo(f"AI confirmation features JSON：{paths['json']}")
    typer.echo(f"AI confirmation features CSV：{paths['csv']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo(f"ai_data_availability_status={availability['status']}")


@ai_confirmation_app.command("report")
def ai_confirmation_report_command(
    prices_path: Annotated[Path, typer.Option(help="ETF / AI 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="AI confirmation report 输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR
    ),
    universe_path: Annotated[Path, typer.Option(help="AI confirmation universe config。")] = (
        DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH
    ),
    policy_path: Annotated[Path, typer.Option(help="AI confirmation scoring policy config。")] = (
        DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH
    ),
    events_path: Annotated[
        Path | None,
        typer.Option(help="可选 AI event calendar JSON/CSV。缺失时按 no active events 处理。"),
    ] = None,
) -> None:
    """生成 TRADING-066G standalone AI confirmation JSON/Markdown report。"""
    config = load_etf_config_bundle()
    ai_config = load_ai_confirmation_universe_config(universe_path)
    policy_config = load_ai_confirmation_policy_config(policy_path)
    extra_symbols = set(all_enabled_price_tickers(ai_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI confirmation report。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    availability = validate_ai_confirmation_data_availability(
        ai_config,
        _available_price_symbols(prices, run_date),
        group_ids=ai_confirmation_price_group_ids(ai_config),
    )
    if availability["status"] == "FAIL":
        typer.echo("AI confirmation 数据覆盖状态：FAIL，已停止 report build。")
        for error in availability["errors"]:
            typer.echo(f"- {error}")
        raise typer.Exit(code=1)
    report_metadata = _quality_metadata(quality_report)
    payload = build_ai_confirmation_report(
        prices=prices,
        events=load_ai_confirmation_events(events_path),
        universe_config=ai_config,
        policy_config=policy_config,
        run_date=run_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range=_price_requested_date_range(prices, run_date),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"ai_confirmation_report_{run_date.isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_ai_confirmation_report(payload, json_path=json_path, markdown_path=markdown_path)
    typer.echo(f"AI confirmation report JSON：{json_path}")
    typer.echo(f"AI confirmation report Markdown：{markdown_path}")
    typer.echo(f"AIConfirmationScore={payload['AIConfirmationScore']['score_value']}")
    typer.echo(f"score_band={payload['AIConfirmationScore']['score_band']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@ai_confirmation_app.command("overlay")
def ai_confirmation_overlay_command(
    candidate_id: Annotated[
        str,
        typer.Option("--candidate", help="Base candidate id for the shadow overlay output."),
    ],
    base_weights_path: Annotated[
        Path,
        typer.Option(help="JSON/YAML/CSV base candidate weights; read-only input."),
    ],
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    ai_confirmation_report_path: Annotated[
        Path | None,
        typer.Option(help="AI confirmation report JSON；缺省读取 latest report。"),
    ] = None,
    report_dir: Annotated[Path, typer.Option(help="AI confirmation report 查找目录。")] = (
        DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="shadow overlay 输出目录。")] = (
        DEFAULT_AI_CONFIRMATION_OVERLAY_DIR
    ),
    policy_path: Annotated[Path, typer.Option(help="AI confirmation scoring policy config。")] = (
        DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH
    ),
) -> None:
    """生成 TRADING-066H candidate-only shadow overlay；不写 production weights。"""
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    report_path = ai_confirmation_report_path or latest_ai_confirmation_report_path(
        report_dir,
        as_of=run_date if date_option else None,
    )
    if report_path is None:
        typer.echo("AI confirmation report not found; run report before overlay.")
        raise typer.Exit(code=1)
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    if isinstance(report_payload, dict) and report_payload.get("date"):
        run_date = date.fromisoformat(str(report_payload["date"]))
    overlay = build_ai_confirmation_shadow_overlay_experiment(
        base_weights=load_ai_confirmation_base_weights(base_weights_path),
        ai_confirmation_payload=report_payload,
        policy_config=load_ai_confirmation_policy_config(policy_path),
        run_date=run_date,
        base_candidate_id=candidate_id,
    )
    stem = f"ai_confirmation_overlay_{run_date.isoformat()}_{candidate_id}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_ai_confirmation_shadow_overlay(overlay, json_path=json_path, markdown_path=markdown_path)
    typer.echo(f"AI confirmation shadow overlay JSON：{json_path}")
    typer.echo(f"AI confirmation shadow overlay Markdown：{markdown_path}")
    typer.echo(f"AIConfirmationScore={overlay['AIConfirmationScore']}")
    typer.echo(f"overlay_direction={overlay['overlay_adjustment']['direction']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@satellite_app.command("evaluate")
def satellite_evaluate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """评估 P1 satellite stock candidates；observe-only，不改 ETF 目标权重。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    quality_metadata = _p1_quality_metadata(prices_path, config, include_satellites=True)
    features = load_feature_store(features_path)
    signals = load_signals(signals_path)
    regimes = load_regimes(regime_path)
    run_date = _resolve_feature_date(date_option, features)
    regime = select_regime_for_date(regimes, run_date)
    candidates = evaluate_satellite_candidates(
        features,
        signals,
        config=config,
        p1_config=config.p1,
        run_date=run_date,
        regime=str(regime["regime"]),
    )
    csv_path = output_dir / f"{run_date.isoformat()}_satellite_candidates.csv"
    md_path = output_dir / f"{run_date.isoformat()}_satellite_candidates.md"
    write_frame_and_report(
        candidates,
        csv_path,
        md_path,
        "ETF Satellite Candidate Report",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF satellite candidates：{md_path}")


@attribution_app.command("report")
def attribution_report_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 portfolio attribution report。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 attribution。")
        raise typer.Exit(code=1)
    quality_metadata = _quality_metadata(quality_report)
    run_date = _resolve_date(date_option, prices=prices)
    allocation = load_allocation(allocation_path)
    allocation_dates = pd.to_datetime(allocation["date"], errors="coerce")
    allocation = allocation.loc[allocation_dates == pd.Timestamp(run_date)]
    attribution = build_portfolio_attribution(allocation, prices, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_attribution.csv"
    md_path = output_dir / f"{run_date.isoformat()}_attribution.md"
    write_frame_and_report(
        attribution,
        csv_path,
        md_path,
        "ETF Portfolio Attribution",
        metadata=quality_metadata,
    )
    typer.echo(f"ETF attribution：{md_path}")


@events_app.command("risk-flag")
def event_risk_flag_command(
    date_option: Annotated[str | None, typer.Option("--date", help="日期或 latest。")] = None,
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 basic event calendar risk flags。"""
    config = load_etf_config_bundle()
    if config.p1 is None:
        raise typer.BadParameter("缺少 ETF P1 config")
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    flags = evaluate_event_risk(p1_config=config.p1, run_date=run_date)
    csv_path = output_dir / f"{run_date.isoformat()}_event_risk_flags.csv"
    md_path = output_dir / f"{run_date.isoformat()}_event_risk_flags.md"
    write_frame_and_report(flags, csv_path, md_path, "ETF Event Risk Flags")
    typer.echo(f"ETF event risk flags：{md_path}")


@governance_app.command("status")
def governance_status_command(
    output_dir: Annotated[Path, typer.Option(help="输出目录。")] = DEFAULT_ETF_REPORT_DIR / "p1",
) -> None:
    """生成 P1 weight governance status；manual-review-only。"""
    config = load_etf_config_bundle()
    status = build_governance_status(config)
    run_date = date.today()
    csv_path = output_dir / f"{run_date.isoformat()}_governance_status.csv"
    md_path = output_dir / f"{run_date.isoformat()}_governance_status.md"
    write_frame_and_report(status, csv_path, md_path, "ETF Weight Governance Status")
    typer.echo(f"ETF governance status：{md_path}")


@governance_app.command("summary")
def governance_summary_command(
    candidate_path: Annotated[
        Path | None,
        typer.Option("--candidate", help="候选参数 JSON；缺省时输出 NO_CANDIDATE。"),
    ] = None,
    date_option: Annotated[str | None, typer.Option("--date", help="报告日期。")] = None,
    policy_path: Annotated[
        Path,
        typer.Option(help="ETF 参数治理 policy。"),
    ] = DEFAULT_ETF_PARAMETER_GOVERNANCE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出目录。"),
    ] = DEFAULT_ETF_REPORT_DIR / "governance",
) -> None:
    """生成 ETF parameter governance summary；只允许人工复核，不自动 promotion。"""
    config = load_etf_config_bundle()
    if candidate_path is not None and not candidate_path.exists():
        raise typer.BadParameter(f"候选参数 JSON 不存在：{candidate_path}")
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    policy = load_parameter_governance_policy(policy_path)
    candidate = read_parameter_candidate(candidate_path)
    payload = evaluate_parameter_candidate(
        config=config,
        policy=policy,
        candidate=candidate,
        candidate_path=candidate_path,
        policy_config_path=policy_path,
    )
    json_path = output_dir / f"{run_date.isoformat()}_parameter_governance.json"
    md_path = output_dir / f"{run_date.isoformat()}_parameter_governance.md"
    write_parameter_governance_summary(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF parameter governance：{md_path}")
    typer.echo(f"promotion_status={payload['promotion_status']}")
    typer.echo(f"production_effect={payload['production_effect']}")


@credibility_app.command("validate")
def credibility_validate_command(
    date_option: Annotated[str | None, typer.Option("--date", help="报告日期。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出目录。"),
    ] = DEFAULT_ETF_CREDIBILITY_DIR,
) -> None:
    """聚合 TRADING-063A~J credibility checks；fail closed。"""
    config = load_etf_config_bundle()
    run_date = _parse_date(date_option) if date_option and date_option != "latest" else date.today()
    payload = build_credibility_gate(config=config)
    json_path = output_dir / f"{run_date.isoformat()}_credibility_gate.json"
    md_path = output_dir / f"{run_date.isoformat()}_credibility_gate.md"
    write_credibility_gate(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF credibility gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"production_effect={payload['production_effect']}")
    typer.echo(f"broker_action={payload['broker_action']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@experiments_app.command("register")
def experiments_register_command(
    status: Annotated[
        str,
        typer.Option(help="实验状态：candidate/shadow/retired 等。"),
    ] = "candidate",
    notes: Annotated[str, typer.Option(help="实验备注。")] = "manual registration",
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
) -> None:
    """登记 ETF experiment registry 记录；不触发 promotion。"""
    config = load_etf_config_bundle()
    append_experiment_registry(
        registry_path=registry_path,
        model_version=config.strategy.model.version,
        parent_model_version=config.strategy.model.version,
        config_hash=config.config_hash,
        parameter_diff={},
        metrics={},
        status=status,
        notes=notes,
    )
    typer.echo(f"ETF experiment registry 已更新：{registry_path}")


@experiments_app.command("run")
def experiments_run_command(
    config_path: Annotated[
        Path | None,
        typer.Option("--config", help="候选 ETF strategy/config YAML；旧 P1 registry 路径。"),
    ] = None,
    pack: Annotated[
        str | None,
        typer.Option("--pack", help="TRADING-064 experiment pack id。"),
    ] = None,
    experiment: Annotated[
        str | None,
        typer.Option("--experiment", help="TRADING-064 single experiment id。"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", help="TRADING-064 batch backtest start YYYY-MM-DD。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="TRADING-064 batch backtest end YYYY-MM-DD。"),
    ] = None,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="TRADING-064 batch run 输出目录。"),
    ] = DEFAULT_ETF_EXPERIMENT_RUN_DIR,
    metrics_path: Annotated[
        Path | None,
        typer.Option("--backtest-summary-path", help="可选：候选回测 summary.json。"),
    ] = None,
    baseline_config_path: Annotated[
        Path,
        typer.Option(help="production baseline strategy config，用于 diff。"),
    ] = DEFAULT_ETF_STRATEGY_CONFIG_PATH,
    status: Annotated[
        str,
        typer.Option(help="实验状态：candidate/shadow/retired 等。"),
    ] = "candidate",
    notes: Annotated[str, typer.Option(help="实验备注。")] = "candidate experiment run",
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
) -> None:
    """记录 P1 candidate run，或执行 TRADING-064 controlled experiment batch。"""
    config = load_etf_config_bundle()
    if pack is not None or experiment is not None:
        if config_path is not None:
            raise typer.BadParameter("--config cannot be combined with --pack/--experiment")
        if start is None or end is None:
            raise typer.BadParameter("--start and --end are required for batch experiments")
        prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
        if not quality_report.passed:
            typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 experiment batch。")
            raise typer.Exit(code=1)
        experiment_registry = load_experiment_registry()
        pack_registry = load_experiment_pack_registry(experiment_registry=experiment_registry)
        batch = run_experiment_batch(
            prices,
            base_config=config,
            quality_report=quality_report,
            experiment_registry=experiment_registry,
            pack_registry=pack_registry,
            pack_id=pack,
            experiment_id=experiment,
            start=_parse_date(start),
            end=_parse_date(end),
            output_root=output_dir,
        )
        typer.echo(f"ETF experiment batch 完成：{batch.run_id}")
        typer.echo(f"Run dir：{batch.run_dir}")
        typer.echo(f"status={batch.diagnostics_summary['status']}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        if batch.diagnostics_summary["status"] != "PASS":
            raise typer.Exit(code=1)
        return
    if config_path is None:
        raise typer.BadParameter("--config or --pack/--experiment is required")
    append_experiment_run(
        registry_path=registry_path,
        candidate_config_path=config_path,
        baseline_config_path=baseline_config_path,
        config=config,
        metrics_path=metrics_path,
        status=status,
        notes=notes,
    )
    typer.echo(f"ETF experiment run 已登记：{registry_path}")
    typer.echo("production_effect=none")


@experiments_app.command("compare")
def experiments_compare_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    baseline: Annotated[str, typer.Option(help="比较基准，默认 production。")] = "production",
    baseline_metrics_path: Annotated[
        Path | None,
        typer.Option("--baseline-summary-path", help="可选：production 回测 summary.json。"),
    ] = None,
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
    output_dir: Annotated[Path, typer.Option(help="比较报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments"
    ),
) -> None:
    """只读比较 ETF experiment registry 或 TRADING-064 batch run；不自动 promotion。"""
    if run_id is not None or latest:
        if run_id is not None and latest:
            raise typer.BadParameter("--run-id and --latest cannot be combined")
        run_dir = (
            find_latest_experiment_run_dir(output_dir)
            if latest
            else output_dir / str(run_id)
        )
        payload = build_experiment_comparison_report(run_dir)
        pack_id = payload["run_metadata"].get("pack_id")
        if pack_id:
            pack_registry = load_experiment_pack_registry()
            pack_config = pack_registry.experiment_packs.get(str(pack_id))
            if pack_config is not None:
                payload = apply_ranking_policy_to_comparison_report(
                    payload,
                    ranking_policy=pack_registry.ranking_policies[
                        pack_config.ranking_policy
                    ],
                    ranking_policy_id=pack_config.ranking_policy,
                )
        json_path = run_dir / "comparison_report.json"
        md_path = run_dir / "comparison_report.md"
        write_experiment_comparison_report(payload, json_path=json_path, markdown_path=md_path)
        typer.echo(f"ETF experiment comparison report：{md_path}")
        typer.echo(f"run_id={payload['run_metadata']['run_id']}")
        typer.echo(f"production_effect={payload['production_effect']}")
        return
    frame = build_experiment_comparison(
        registry_path=registry_path,
        baseline=baseline,
        baseline_metrics_path=baseline_metrics_path,
    )
    run_date = date.today()
    csv_path = output_dir / f"{run_date.isoformat()}_experiment_compare.csv"
    md_path = output_dir / f"{run_date.isoformat()}_experiment_compare.md"
    write_frame_and_report(
        frame,
        csv_path,
        md_path,
        "ETF Experiment Comparison",
        metadata={
            "baseline": baseline,
            "registry_path": registry_path,
            "production_effect": "none",
            "auto_promotion": False,
        },
    )
    typer.echo(f"ETF experiment comparison：{md_path}")
    typer.echo("production_effect=none")


@experiments_app.command("select-candidates")
def experiments_select_candidates_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    promotion_policy: Annotated[
        str | None,
        typer.Option("--promotion-policy", help="覆盖默认 pack promotion policy。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="experiment run 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
) -> None:
    """生成 TRADING-064 candidate selection gate；只允许 shadow-only/manual-review。"""
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    run_dir = find_latest_experiment_run_dir(output_dir) if latest else output_dir / str(run_id)
    selection = _build_experiment_candidate_selection(
        run_dir,
        promotion_policy_override=promotion_policy,
    )
    json_path = run_dir / "candidate_selection_report.json"
    md_path = run_dir / "candidate_selection_report.md"
    write_candidate_selection_report(selection, json_path=json_path, markdown_path=md_path)
    summary = selection["selection_summary"]
    typer.echo(f"ETF experiment candidate selection gate：{md_path}")
    typer.echo(f"run_id={selection['run_metadata'].get('run_id')}")
    typer.echo(f"status={summary['status']}")
    typer.echo(f"eligible_for_shadow={summary['eligible_for_shadow_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo(f"production_effect={selection['production_effect']}")


@experiments_app.command("enroll-shadow")
def experiments_enroll_shadow_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    candidate: Annotated[
        list[str] | None,
        typer.Option("--candidate", help="candidate_id 或 experiment_id，可重复。"),
    ] = None,
    top: Annotated[
        int | None,
        typer.Option("--top", help="登记前 N 个 eligible_for_shadow candidates。"),
    ] = None,
    promotion_policy: Annotated[
        str | None,
        typer.Option("--promotion-policy", help="覆盖默认 pack promotion policy。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="experiment run 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 输出路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
) -> None:
    """把 eligible ETF experiment candidate 登记到 observe-only shadow registry。"""
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    run_dir = find_latest_experiment_run_dir(output_dir) if latest else output_dir / str(run_id)
    selection = _build_experiment_candidate_selection(
        run_dir,
        promotion_policy_override=promotion_policy,
    )
    write_candidate_selection_report(
        selection,
        json_path=run_dir / "candidate_selection_report.json",
        markdown_path=run_dir / "candidate_selection_report.md",
    )
    try:
        registry = enroll_shadow_candidates(
            selection,
            registry_path=registry_path,
            candidate_ids=candidate,
            top=top,
        )
    except ValueError as exc:
        typer.echo(f"ETF shadow enrollment blocked: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF shadow candidates registry：{registry_path}")
    typer.echo(f"candidate_count={registry['candidate_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@experiments_app.command("weekly-review")
def experiments_weekly_review_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="周度复核日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 latest weekly review。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    run_root: Annotated[Path, typer.Option(help="experiment run 根目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_WEEKLY_REVIEW_DIR
    ),
    review_policy: Annotated[
        str,
        typer.Option("--review-policy", help="weekly review policy id。"),
    ] = "weekly_shadow_review_v1",
) -> None:
    """生成 observe-only ETF experiment weekly review；不允许 production promotion。"""
    if latest and as_of is not None:
        raise typer.BadParameter("--latest and --as-of cannot be combined")
    review_date = date.today() if latest else _parse_date(as_of)
    pack_registry = load_experiment_pack_registry()
    policy = pack_registry.review_policies.get(review_policy)
    if policy is None:
        raise typer.BadParameter(f"unknown review policy: {review_policy}")
    payload = build_weekly_experiment_review(
        as_of=review_date,
        shadow_registry_path=registry_path,
        run_root=run_root,
        review_policy=policy,
        review_policy_id=review_policy,
    )
    json_path = output_dir / f"weekly_review_{review_date.isoformat()}.json"
    md_path = output_dir / f"weekly_review_{review_date.isoformat()}.md"
    write_weekly_experiment_review_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF experiment weekly review：{md_path}")
    typer.echo(f"status={payload['summary']['status']}")
    typer.echo(f"candidate_count={payload['summary']['candidate_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo(f"production_effect={payload['production_effect']}")


@experiments_app.command("validate")
def experiments_validate_command(
    pack: Annotated[
        str,
        typer.Option("--pack", help="TRADING-064 experiment pack id。"),
    ] = "etf_calibration_v1",
    output_dir: Annotated[Path, typer.Option(help="validation report 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR / "validation"
    ),
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = PROJECT_ROOT / "config" / "report_registry.yaml",
) -> None:
    """生成 TRADING-064 final experiment validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_experiment_validation_report(
        pack_id=pack,
        report_registry_path=report_registry_path,
        generated_at=generated,
    )
    safe_pack = pack.replace("/", "_").replace("\\", "_")
    stem = f"{generated.date().isoformat()}_{safe_pack}_experiment_validation"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_experiment_validation_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF experiment validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@forward_app.command("update")
def forward_update_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="forward evaluation 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 ETF price cache 最新日期。"),
    ] = False,
    config_path: Annotated[
        Path,
        typer.Option(help="TRADING-065 forward simulation policy config。"),
    ] = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Annotated[
        Path,
        typer.Option(help="decision-time ledger 输出路径。"),
    ] = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    output_dir: Annotated[Path, typer.Option(help="forward update 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "updates"
    ),
) -> None:
    """更新 active shadow candidates 的 evaluation-only forward performance。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = _resolve_date(
        "latest" if latest or date_option is None else date_option,
        prices_path=prices_path,
    )
    try:
        payload = run_forward_update(
            as_of=run_date,
            config_path=config_path,
            registry_path=registry_path,
            decision_ledger_path=decision_ledger_path,
            prices_path=prices_path,
            output_dir=output_dir,
        )
    except ValueError as exc:
        typer.echo(f"ETF forward update blocked: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF forward update：{output_dir / f'forward_update_{run_date.isoformat()}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['active_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@forward_app.command("dashboard")
def forward_dashboard_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="dashboard 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest forward update artifact。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    update_dir: Annotated[Path, typer.Option(help="forward update artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "updates"
    ),
    output_dir: Annotated[Path, typer.Option(help="dashboard 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
) -> None:
    """生成 candidate vs baseline vs benchmark forward dashboard。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_dashboard(
        as_of=run_date,
        latest=latest or date_option is None,
        registry_path=registry_path,
        update_dir=update_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward dashboard：{output_dir / f'forward_dashboard_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['status_summary']['active_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@forward_app.command("weekly-review")
def forward_weekly_review_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="weekly review 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest dashboard artifact。"),
    ] = False,
    dashboard_dir: Annotated[Path, typer.Option(help="forward dashboard artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "weekly_reviews"
    ),
) -> None:
    """生成 TRADING-065 observe-only weekly review。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_weekly_review(
        as_of=run_date,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward weekly review：{output_dir / f'weekly_review_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['review_period']['candidate_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@forward_app.command("watchlist")
def forward_watchlist_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="watchlist 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest dashboard artifact。"),
    ] = False,
    dashboard_dir: Annotated[Path, typer.Option(help="forward dashboard artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
    output_dir: Annotated[Path, typer.Option(help="watchlist 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "watchlist"
    ),
) -> None:
    """生成本地 ETF forward watchlist；不发送外部 alert。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_watchlist(
        as_of=run_date,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward watchlist：{output_dir / f'forward_watchlist_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"attention_count={payload['summary']['item_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@forward_app.command("validate")
def forward_validate_command(
    config_path: Annotated[
        Path,
        typer.Option(help="TRADING-065 forward simulation policy config。"),
    ] = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Annotated[
        Path,
        typer.Option(help="decision-time ledger 路径。"),
    ] = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = PROJECT_ROOT / "config" / "report_registry.yaml",
    output_dir: Annotated[Path, typer.Option(help="validation 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "validation"
    ),
) -> None:
    """生成 TRADING-065 final forward simulation validation gate。"""
    payload = run_forward_validation(
        config_path=config_path,
        registry_path=registry_path,
        decision_ledger_path=decision_ledger_path,
        report_registry_path=report_registry_path,
        output_dir=output_dir,
    )
    typer.echo(f"ETF forward validation gate：{output_dir}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


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
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = (
        "manual_input"
    ),
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
    source_url: Annotated[str, typer.Option(help="Source URL 或人工输入说明。")] = (
        "manual_input"
    ),
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


@signals_app.command("generate")
def signals_generate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="信号日期或 latest。")] = None,
    output_path: Annotated[Path, typer.Option(help="信号输出路径。")] = DEFAULT_ETF_SIGNAL_PATH,
) -> None:
    """生成 ETF signals，不输出仓位。"""
    config = load_etf_config_bundle()
    features = load_feature_store(features_path)
    run_date = _resolve_feature_date(date_option, features)
    records = generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    write_signals(records, output_path)
    typer.echo(f"ETF signals 已写入：{output_path}（date={run_date.isoformat()}）")


@regime_app.command("generate")
def regime_generate_command(
    features_path: Annotated[Path, typer.Option(help="Feature store 路径。")] = (
        DEFAULT_ETF_FEATURE_PATH
    ),
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    date_option: Annotated[
        str | None,
        typer.Option("--date", help="regime 日期或 latest。"),
    ] = None,
    output_path: Annotated[Path, typer.Option(help="Regime 输出路径。")] = DEFAULT_ETF_REGIME_PATH,
) -> None:
    """生成 ETF market regime。"""
    config = load_etf_config_bundle()
    features = load_feature_store(features_path)
    signals = load_signals(signals_path)
    run_date = _resolve_feature_date(date_option, features)
    record = generate_regime_for_date(
        features,
        signals,
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )
    write_regime(record, output_path)
    typer.echo(f"ETF regime 已写入：{output_path}（{record.regime}）")


@portfolio_app.command("allocate")
def portfolio_allocate_command(
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径，用于质量门禁。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="组合日期或 latest。")] = None,
    output_path: Annotated[Path, typer.Option(help="目标权重输出路径。")] = DEFAULT_ETF_TARGET_PATH,
) -> None:
    """由 ETF signals + regime 生成目标权重。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 allocation。")
        raise typer.Exit(code=1)
    signals = load_signals(signals_path)
    regimes = load_regimes(regime_path)
    run_date = _resolve_date(date_option, prices=prices)
    signal_rows = select_signals_for_date(signals, run_date)
    regime_row = select_regime_for_date(regimes, run_date)
    previous_weights = latest_weights_from_file(output_path)
    records = allocate_portfolio(
        signal_rows,
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime=str(regime_row["regime"]),
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=quality_report,
        previous_weights=previous_weights,
    )
    write_allocation(records, output_path)
    typer.echo(f"ETF target weights 已写入：{output_path}")
    typer.echo(f"sum={sum(record.target_weight for record in records):.6f}")


@backtest_app.command("run")
def backtest_run_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="ETF backtest config YAML 路径。"),
    ] = DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    start: Annotated[str | None, typer.Option("--from", help="回测开始日期。")] = None,
    end: Annotated[str | None, typer.Option("--to", help="回测结束日期。")] = None,
    output_dir: Annotated[Path, typer.Option(help="回测输出目录。")] = DEFAULT_ETF_BACKTEST_DIR,
    fast: Annotated[bool, typer.Option("--fast", help="只跑最近约 90 个交易日 smoke。")] = False,
) -> None:
    """运行 ETF 组合级回测。"""
    config = load_etf_config_bundle(backtest_path=config_path)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 backtest。")
        raise typer.Exit(code=1)
    run = run_portfolio_backtest(
        prices,
        config=config,
        quality_report=quality_report,
        start=_parse_date(start) if start else None,
        end=_resolve_date(end, prices=prices) if end else None,
        fast=fast,
    )
    (
        daily_path,
        weights_path,
        trades_path,
        summary_json,
        metrics_json,
        summary_md,
        stability_json,
        stability_md,
    ) = write_backtest_run(run, output_dir)
    typer.echo(f"ETF backtest 完成：{run.run_id}")
    typer.echo(f"Daily：{daily_path}")
    typer.echo(f"Weights：{weights_path}")
    typer.echo(f"Trades：{trades_path}")
    typer.echo(f"Summary JSON：{summary_json}")
    typer.echo(f"Metrics JSON：{metrics_json}")
    typer.echo(f"Summary Markdown：{summary_md}")
    typer.echo(f"Stability JSON：{stability_json}")
    typer.echo(f"Stability Markdown：{stability_md}")


@backtest_app.command("report")
def backtest_report_command(
    run_id: Annotated[str, typer.Option(help="回测 run_id。")],
    output_dir: Annotated[Path, typer.Option(help="回测输出根目录。")] = DEFAULT_ETF_BACKTEST_DIR,
) -> None:
    """显示已生成 ETF backtest summary 路径。"""
    summary = output_dir / run_id / "summary.md"
    if not summary.exists():
        raise typer.BadParameter(f"未找到 ETF backtest summary：{summary}")
    typer.echo(str(summary))


@backtest_app.command("diagnostics")
def backtest_diagnostics_command(
    run_id: Annotated[str | None, typer.Option(help="回测 run_id；省略时使用 latest。")] = None,
    latest: Annotated[bool, typer.Option("--latest", help="读取最新 backtest run。")] = False,
    output_dir: Annotated[Path, typer.Option(help="回测输出根目录。")] = DEFAULT_ETF_BACKTEST_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="ETF backtest config YAML 路径。"),
    ] = DEFAULT_ETF_BACKTEST_CONFIG_PATH,
) -> None:
    """从已生成 backtest run 计算 allocation stability diagnostics。"""
    selected_run_dir = _resolve_backtest_run_dir(output_dir, run_id=run_id, latest=latest)
    daily_path = selected_run_dir / "daily.csv"
    weights_path = selected_run_dir / "weights.csv"
    if not daily_path.exists() or not weights_path.exists():
        raise typer.BadParameter(f"backtest run 缺少 daily.csv 或 weights.csv：{selected_run_dir}")
    config = load_etf_config_bundle(backtest_path=config_path)
    payload = build_allocation_stability_diagnostics(
        pd.read_csv(daily_path),
        pd.read_csv(weights_path),
        max_daily_turnover=config.risk.portfolio_constraints.max_daily_turnover,
        max_rebalance_trade_weight=config.risk.portfolio_constraints.max_rebalance_trade_weight,
    )
    json_path, markdown_path = write_allocation_stability_diagnostics(
        payload,
        selected_run_dir / "stability_diagnostics.json",
        selected_run_dir / "stability_diagnostics.md",
    )
    typer.echo(f"ETF allocation stability status：{payload['status']}")
    typer.echo(f"JSON：{json_path}")
    typer.echo(f"Markdown：{markdown_path}")


@simulation_app.command("record")
def simulation_record_command(
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    date_option: Annotated[
        str | None,
        typer.Option("--date", help="记录日期或 latest；默认 latest allocation date。"),
    ] = None,
    report_path: Annotated[Path | None, typer.Option(help="关联日报路径。")] = None,
) -> None:
    """记录 ETF 模拟舱快照，按 date/model_version/symbol 幂等 upsert。"""
    allocation = load_allocation(allocation_path)
    run_date = _resolve_frame_date(date_option or "latest", allocation)
    allocation = _select_frame_date(allocation, run_date, label="目标权重")
    records = [_allocation_record_from_row(row) for _, row in allocation.iterrows()]
    record_simulation_snapshot(
        allocation_records=records,
        ledger_path=ledger_path,
        report_path=report_path,
    )
    typer.echo(f"ETF simulation ledger 已更新：{ledger_path}（date={run_date.isoformat()}）")


@simulation_app.command("evaluate")
def simulation_evaluate_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
) -> None:
    """补充 ETF 模拟舱 forward return 字段；未来窗口不足保持 null。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 simulation evaluate。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    evaluate_simulation_ledger(ledger_path=ledger_path, prices=prices, as_of=run_date)
    typer.echo(f"ETF simulation ledger 已评估：{ledger_path}")


@simulation_app.command("report")
def simulation_report_command(
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    window: Annotated[str, typer.Option(help="报告窗口。")] = "60d",
    output_path: Annotated[Path | None, typer.Option(help="报告输出路径。")] = None,
) -> None:
    """生成 ETF 模拟舱报告。"""
    report_path = output_path or DEFAULT_ETF_REPORT_DIR / f"simulation_report_{window}.md"
    write_simulation_report(ledger_path, report_path, window=window)
    typer.echo(f"ETF simulation report：{report_path}")


@report_app.command("daily")
def report_daily_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    signals_path: Annotated[Path, typer.Option(help="Signals 路径。")] = DEFAULT_ETF_SIGNAL_PATH,
    regime_path: Annotated[Path, typer.Option(help="Regime 路径。")] = DEFAULT_ETF_REGIME_PATH,
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    date_option: Annotated[str | None, typer.Option("--date", help="日报日期或 latest。")] = None,
    output_path: Annotated[Path | None, typer.Option(help="日报输出路径。")] = None,
) -> None:
    """生成 ETF Daily Portfolio Brief。"""
    report_path = _generate_daily_report(
        prices_path=prices_path,
        signals_path=signals_path,
        regime_path=regime_path,
        allocation_path=allocation_path,
        ledger_path=ledger_path,
        date_option=date_option,
        output_path=output_path,
    )
    typer.echo(f"ETF daily brief：{report_path}")


@run_app.command("daily")
def run_daily_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    date_option: Annotated[str | None, typer.Option("--date", help="运行日期或 latest。")] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="写入 dry-run artifact 目录。"),
    ] = False,
) -> None:
    """执行 ETF P0 日流程：validate -> features -> signals -> regime -> allocation -> report。"""
    root = (
        PROJECT_ROOT / "artifacts" / "etf_portfolio" / "dry_run"
        if dry_run
        else PROJECT_ROOT / "data" / "etf_portfolio"
    )
    report_root = (
        PROJECT_ROOT / "artifacts" / "etf_portfolio" / "dry_run_reports"
        if dry_run
        else DEFAULT_ETF_REPORT_DIR
    )
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 daily run。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(date_option, prices=prices)
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    features_path = root / "features.csv"
    signals_path = root / "signals.csv"
    regime_path = root / "regimes.csv"
    allocation_path = root / "target_weights.csv"
    write_feature_store(features, features_path)
    signal_records = generate_signals_for_date(
        features,
        strategy=config.strategy,
        run_date=run_date,
    )
    write_signals(signal_records, signals_path)
    regime_record = generate_regime_for_date(
        features,
        signals_to_frame(signal_records),
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )
    write_regime(regime_record, regime_path)
    allocation_records = allocate_portfolio(
        signals_to_frame(signal_records),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime=regime_record.regime,
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=quality_report,
        previous_weights=None if dry_run else latest_weights_from_file(DEFAULT_ETF_TARGET_PATH),
    )
    write_allocation(allocation_records, allocation_path)
    report_path = _generate_daily_report(
        prices_path=prices_path,
        signals_path=signals_path,
        regime_path=regime_path,
        allocation_path=allocation_path,
        ledger_path=DEFAULT_ETF_LEDGER_PATH,
        date_option=run_date.isoformat(),
        output_path=report_root / f"{run_date.isoformat()}_portfolio_brief.md",
    )
    if not dry_run:
        record_simulation_snapshot(
            allocation_records=allocation_records,
            ledger_path=DEFAULT_ETF_LEDGER_PATH,
            report_path=report_path,
        )
        evaluate_simulation_ledger(
            ledger_path=DEFAULT_ETF_LEDGER_PATH,
            prices=prices,
            as_of=run_date,
        )
    typer.echo(f"ETF daily run 完成：{run_date.isoformat()}")
    typer.echo(f"dry_run={str(dry_run).lower()}")
    typer.echo(f"report={report_path}")
    typer.echo(f"data_quality_status={quality_report.status}")


def _generate_daily_report(
    *,
    prices_path: Path,
    signals_path: Path,
    regime_path: Path,
    allocation_path: Path,
    ledger_path: Path | None,
    date_option: str | None,
    output_path: Path | None,
) -> Path:
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    run_date = _resolve_date(date_option, prices=prices)
    signals = select_signals_for_date(load_signals(signals_path), run_date)
    regime = select_regime_for_date(load_regimes(regime_path), run_date)
    allocation = load_allocation(allocation_path)
    allocation_dates = pd.to_datetime(allocation["date"], errors="coerce")
    allocation = allocation.loc[allocation_dates == pd.Timestamp(run_date)]
    if allocation.empty:
        raise typer.BadParameter(f"目标权重缺少日期：{run_date.isoformat()}")
    report_path = (
        output_path
        or DEFAULT_ETF_REPORT_DIR / f"{run_date.isoformat()}_portfolio_brief.md"
    )
    markdown = render_daily_brief(
        run_date=run_date,
        config=config,
        quality_report=quality_report,
        signals=signals,
        regime=regime,
        allocation=allocation,
        simulation_summary=(
            summarize_simulation_for_brief(ledger_path, as_of=run_date)
            if ledger_path is not None
            else None
        ),
    )
    return write_daily_brief(markdown, report_path)


def _build_experiment_candidate_selection(
    run_dir: Path,
    *,
    promotion_policy_override: str | None,
) -> dict[str, object]:
    payload = build_experiment_comparison_report(run_dir)
    pack_registry = load_experiment_pack_registry()
    pack_id = payload["run_metadata"].get("pack_id")
    policy_id = promotion_policy_override or "shadow_only_manual_review"
    if pack_id:
        pack_config = pack_registry.experiment_packs.get(str(pack_id))
        if pack_config is not None:
            payload = apply_ranking_policy_to_comparison_report(
                payload,
                ranking_policy=pack_registry.ranking_policies[pack_config.ranking_policy],
                ranking_policy_id=pack_config.ranking_policy,
            )
            policy_id = promotion_policy_override or pack_config.promotion_policy
    policy = pack_registry.promotion_policies.get(policy_id)
    if policy is None:
        raise typer.BadParameter(f"unknown promotion policy: {policy_id}")
    return build_candidate_selection_report(
        payload,
        promotion_policy=policy,
        promotion_policy_id=policy_id,
    )


def _resolve_date(
    value: str | None,
    *,
    prices_path: Path | None = None,
    prices: pd.DataFrame | None = None,
) -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    if prices is None:
        if prices_path is None:
            raise typer.BadParameter("latest 需要 prices 或 prices_path")
        config = load_etf_config_bundle()
        raw = read_price_frame(prices_path)
        prices, _ = standardize_price_frame(raw, assets=config.assets, source_name=str(prices_path))
    return latest_price_date(prices)


def _resolve_feature_date(value: str | None, features: pd.DataFrame) -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    return latest_feature_date(features)


def _resolve_frame_date(value: str | None, frame: pd.DataFrame, column: str = "date") -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    if column not in frame.columns:
        raise typer.BadParameter(f"{column} 字段不存在")
    parsed = pd.to_datetime(frame[column], errors="coerce").dropna()
    if parsed.empty:
        raise typer.BadParameter(f"{column} 没有可用日期")
    return parsed.max().date()


def _resolve_backtest_run_dir(output_dir: Path, *, run_id: str | None, latest: bool) -> Path:
    if run_id is not None and latest:
        raise typer.BadParameter("run_id 和 --latest 只能选择一个")
    if run_id is not None:
        run_dir = output_dir / run_id
        if not run_dir.exists():
            raise typer.BadParameter(f"未找到 ETF backtest run：{run_dir}")
        return run_dir
    if not output_dir.exists():
        raise typer.BadParameter(f"ETF backtest 输出目录不存在：{output_dir}")
    candidates = [
        item
        for item in output_dir.iterdir()
        if item.is_dir() and (item / "summary.json").exists()
    ]
    if not candidates:
        raise typer.BadParameter(f"未找到 ETF backtest run：{output_dir}")
    return max(candidates, key=lambda item: (item / "summary.json").stat().st_mtime)


def _select_frame_date(
    frame: pd.DataFrame,
    run_date: date,
    *,
    column: str = "date",
    label: str = "数据",
) -> pd.DataFrame:
    if column not in frame.columns:
        raise typer.BadParameter(f"{label}缺少 {column} 字段")
    parsed = pd.to_datetime(frame[column], errors="coerce")
    selected = frame.loc[parsed == pd.Timestamp(run_date)].copy()
    if selected.empty:
        raise typer.BadParameter(f"{label}缺少日期：{run_date.isoformat()}")
    return selected


def _resolve_p2_source_date(value: str | None, path: Path, as_of_column: str) -> date:
    if value is not None and value != "latest":
        return _parse_date(value)
    if not path.exists():
        return date.today()
    frame = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)
    if as_of_column not in frame.columns:
        return date.today()
    return _resolve_frame_date("latest", frame, as_of_column)


def _parse_date(value: str | None) -> date:
    if value is None:
        raise typer.BadParameter("日期不能为空")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 或 latest") from exc


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = pd.Timestamp(value).to_pydatetime()
    except ValueError as exc:
        raise typer.BadParameter("时间必须使用 ISO-8601 格式") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _allocation_record_from_row(row: pd.Series) -> ETFAllocationRecord:
    return ETFAllocationRecord(
        date=_parse_date(str(row["date"])),
        symbol=str(row["symbol"]),
        target_weight=float(row["target_weight"]),
        previous_weight=_optional_float(row.get("previous_weight")),
        trade_delta=_optional_float(row.get("trade_delta")),
        composite_score=_optional_float(row.get("composite_score")),
        regime=str(row["regime"]),
        reason_codes=tuple(_json_list(row.get("reason_codes"))),
        constraints_applied=tuple(_json_list(row.get("constraints_applied"))),
        model_version=str(row["model_version"]),
        config_hash=str(row["config_hash"]),
        data_quality_status=str(row["data_quality_status"]),
        created_at=pd.Timestamp(str(row["created_at"])).to_pydatetime(),
        constraint_diagnostics=tuple(_json_records(row.get("constraint_diagnostics"))),
    )


def _optional_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _json_list(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except ValueError:
        return [str(value)]
    if not isinstance(parsed, list):
        return [str(parsed)]
    return [str(item) for item in parsed]


def _json_records(value: object) -> list[dict[str, object]]:
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except ValueError:
        return []
    if not isinstance(parsed, list):
        return []
    return [dict(item) for item in parsed if isinstance(item, dict)]


def _p1_quality_metadata(
    prices_path: Path,
    config,
    *,
    include_satellites: bool,
) -> dict[str, object]:
    _, report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=_satellite_symbols(config) if include_satellites else None,
    )
    if not report.passed:
        typer.echo(f"ETF 数据质量状态：{report.status}，已停止 P1 report。")
        raise typer.Exit(code=1)
    return _quality_metadata(report)


def _quality_metadata(report) -> dict[str, object]:
    report_date = report.max_date.isoformat() if report.max_date else "unknown"
    report_path = DEFAULT_ETF_REPORT_DIR / f"data_quality_{report_date}.md"
    write_quality_report(report, report_path)
    return {
        "data_quality_status": report.status,
        "data_quality_report": f"`{report_path}`",
    }


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


def _satellite_symbols(config) -> set[str]:
    if config.p1 is None:
        return set()
    return set(config.p1.satellite_stocks)


def _available_price_symbols(prices: pd.DataFrame, run_date: date) -> set[str]:
    frame = prices.copy()
    if "date" not in frame.columns or "symbol" not in frame.columns:
        return set()
    parsed_dates = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[parsed_dates.notna() & (parsed_dates <= pd.Timestamp(run_date))]
    return {str(symbol).strip().upper() for symbol in selected["symbol"].dropna()}


def _price_requested_date_range(prices: pd.DataFrame, run_date: date) -> dict[str, str]:
    if "date" not in prices.columns:
        return {"start": "", "end": run_date.isoformat()}
    parsed_dates = pd.to_datetime(prices["date"], errors="coerce")
    selected = parsed_dates.loc[parsed_dates.notna() & (parsed_dates <= pd.Timestamp(run_date))]
    if selected.empty:
        return {"start": "", "end": run_date.isoformat()}
    return {
        "start": selected.min().date().isoformat(),
        "end": run_date.isoformat(),
    }
