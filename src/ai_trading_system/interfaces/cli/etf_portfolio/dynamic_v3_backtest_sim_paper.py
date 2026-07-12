from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_EVENT_DIR,
    DEFAULT_BACKTEST_SIM_PAPER_DIR,
    DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    backtest_sim_paper_report_payload,
    run_backtest_sim_paper,
    validate_backtest_sim_paper_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backtest_sim_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backtest_sim_app.command("paper-run")
def dynamic_v3_backtest_sim_paper_run_command(
    variant_set_id: Annotated[str, typer.Option("--variant-set-id", help="variant set id。")],
    variant: Annotated[
        str,
        typer.Option("--variant", help="paper simulation variant。"),
    ] = "limited_adjustment",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation paper artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_PAPER_DIR,
    variant_dir: Annotated[
        Path,
        typer.Option("--variant-dir", help="backtest simulation variant artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    event_dir: Annotated[
        Path,
        typer.Option("--event-dir", help="backtest simulation event artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_EVENT_DIR,
) -> None:
    """运行 TRADING-164 historical paper portfolio v2。"""
    result = run_backtest_sim_paper(
        variant_set_id=variant_set_id,
        variant=variant,
        output_dir=output_dir,
        variant_dir=variant_dir,
        event_dir=event_dir,
    )
    manifest = result["manifest"]
    summary = result["performance_summary"]
    typer.echo(f"sim_paper_id={result['sim_paper_id']}")
    typer.echo(f"paper_dir={result['sim_paper_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"variant={summary['variant']}")
    typer.echo(f"total_return={summary['total_return']}")
    typer.echo(f"max_drawdown={summary['max_drawdown']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")


@dynamic_v3_backtest_sim_app.command("paper-report")
def dynamic_v3_backtest_sim_paper_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest backtest sim paper。"),
    ] = False,
    sim_paper_id: Annotated[
        str | None,
        typer.Option("--sim-paper-id", help="simulation paper id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation paper artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_PAPER_DIR,
) -> None:
    """展示 TRADING-164 paper portfolio v2 摘要。"""
    payload = backtest_sim_paper_report_payload(
        sim_paper_id=sim_paper_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["sim_paper_performance_summary"]
    typer.echo(f"sim_paper_id={payload['sim_paper_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variant={summary['variant']}")
    typer.echo(f"total_return={summary['total_return']}")
    typer.echo(f"max_drawdown={summary['max_drawdown']}")
    typer.echo(f"report_path={payload['backtest_sim_paper_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backtest-sim-paper")
def dynamic_v3_validate_backtest_sim_paper_command(
    sim_paper_id: Annotated[str, typer.Option("--sim-paper-id", help="simulation paper id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation paper artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_PAPER_DIR,
) -> None:
    """校验 TRADING-164 backtest simulation paper artifact。"""
    payload = validate_backtest_sim_paper_artifact(
        sim_paper_id=sim_paper_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backtest_sim_paper_run_command",
    "dynamic_v3_backtest_sim_paper_report_command",
    "dynamic_v3_validate_backtest_sim_paper_command",
]
