from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.allocation import (
    allocate_portfolio,
    latest_weights_from_file,
    load_allocation,
    write_allocation,
)
from ai_trading_system.etf_portfolio.credibility import (
    DEFAULT_ETF_CREDIBILITY_DIR,
    build_credibility_gate,
    write_credibility_gate,
)
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.features import (
    build_feature_store,
    load_feature_store,
    write_feature_store,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_FEATURE_PATH,
    DEFAULT_ETF_LEDGER_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REGIME_PATH,
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_SIGNAL_PATH,
    DEFAULT_ETF_TARGET_PATH,
    load_etf_config_bundle,
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
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    resolve_feature_date as _resolve_feature_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    credibility_app,
    etf_app,
    portfolio_app,
    regime_app,
    report_app,
    run_app,
    signals_app,
)


@etf_app.command("validate-config")
def validate_config_command() -> None:
    """校验 ETF P0 配置。"""
    config = load_etf_config_bundle()
    typer.echo("ETF 配置校验通过。")
    typer.echo(f"model_version={config.strategy.model.version}")
    typer.echo(f"config_hash={config.config_hash}")
    typer.echo(f"assets={', '.join(config.assets.assets)}")


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
        output_path or DEFAULT_ETF_REPORT_DIR / f"{run_date.isoformat()}_portfolio_brief.md"
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
