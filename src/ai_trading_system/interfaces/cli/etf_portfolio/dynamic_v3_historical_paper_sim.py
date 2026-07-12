from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    DEFAULT_HISTORICAL_REPLAY_DIR,
    historical_paper_sim_report_payload,
    run_historical_paper_sim,
    validate_historical_paper_sim_artifact,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_historical_paper_sim_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_historical_paper_sim_app.command("run")
def dynamic_v3_historical_paper_sim_run_command(
    replay_id: Annotated[str, typer.Option("--replay-id", help="replay id。")],
    variant: Annotated[
        str,
        typer.Option("--variant", help="simulation variant。"),
    ] = "limited_adjustment",
    replay_dir: Annotated[
        Path,
        typer.Option("--replay-dir", help="historical replay artifact root。"),
    ] = DEFAULT_HISTORICAL_REPLAY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="historical paper sim artifact root。"),
    ] = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="cached ETF price path。"),
    ] = DEFAULT_ETF_PRICE_PATH,
) -> None:
    """运行 TRADING-144 historical paper portfolio simulation。"""
    result = run_historical_paper_sim(
        replay_id=replay_id,
        variant=variant,
        replay_dir=replay_dir,
        output_dir=output_dir,
        prices_path=prices_path,
    )
    summary = result["performance_summary"]
    typer.echo(f"sim_id={result['sim_id']}")
    typer.echo(f"sim_dir={result['sim_dir']}")
    typer.echo(f"status={summary['simulation_status']}")
    typer.echo(f"variant={summary['variant']}")
    typer.echo(f"total_return={summary['total_return']}")
    typer.echo(f"max_drawdown={summary['max_drawdown']}")
    typer.echo(f"turnover={summary['turnover']}")
    typer.echo(f"relative_to_no_trade={summary['relative_to_no_trade']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_historical_paper_sim_app.command("report")
def dynamic_v3_historical_paper_sim_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest historical paper sim。"),
    ] = False,
    sim_id: Annotated[str | None, typer.Option("--sim-id", help="simulation id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="historical paper sim artifact root。"),
    ] = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
) -> None:
    """展示 TRADING-144 historical paper sim 摘要。"""
    payload = historical_paper_sim_report_payload(
        sim_id=sim_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload.get("simulated_performance_summary")
    if not isinstance(summary, dict):
        summary = {}
    typer.echo(f"sim_id={payload['sim_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variant={summary.get('variant')}")
    typer.echo(f"total_return={summary.get('total_return')}")
    typer.echo(f"max_drawdown={summary.get('max_drawdown')}")
    typer.echo(f"turnover={summary.get('turnover')}")
    typer.echo(f"report_path={payload['historical_paper_sim_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-historical-paper-sim")
def dynamic_v3_validate_historical_paper_sim_command(
    sim_id: Annotated[str, typer.Option("--sim-id", help="simulation id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="historical paper sim artifact root。"),
    ] = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
) -> None:
    """校验 TRADING-144 historical paper sim artifact。"""
    payload = validate_historical_paper_sim_artifact(sim_id=sim_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_historical_paper_sim_run_command",
    "dynamic_v3_historical_paper_sim_report_command",
    "dynamic_v3_validate_historical_paper_sim_command",
]
