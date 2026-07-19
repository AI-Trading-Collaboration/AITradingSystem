from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from ai_trading_system.etf_portfolio.backtest import run_portfolio_backtest, write_backtest_run
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_BACKTEST_CONFIG_PATH,
    DEFAULT_ETF_BACKTEST_DIR,
    DEFAULT_ETF_PRICE_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.stability import (
    build_allocation_stability_diagnostics,
    write_allocation_stability_diagnostics,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import backtest_app


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
        item for item in output_dir.iterdir() if item.is_dir() and (item / "summary.json").exists()
    ]
    if not candidates:
        raise typer.BadParameter(f"未找到 ETF backtest run：{output_dir}")
    return max(candidates, key=lambda item: (item / "summary.json").stat().st_mtime)
