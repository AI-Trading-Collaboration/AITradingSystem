from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_EVENT_DIR,
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    backtest_sim_outcome_report_payload,
    run_backtest_sim_outcome,
    validate_backtest_sim_outcome_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backtest_sim_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backtest_sim_app.command("outcome-run")
def dynamic_v3_backtest_sim_outcome_run_command(
    variant_set_id: Annotated[str, typer.Option("--variant-set-id", help="variant set id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation outcome artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    variant_dir: Annotated[
        Path,
        typer.Option("--variant-dir", help="backtest simulation variant artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    event_dir: Annotated[
        Path,
        typer.Option("--event-dir", help="backtest simulation event artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_EVENT_DIR,
) -> None:
    """运行 TRADING-163 simulated outcome windows。"""
    result = run_backtest_sim_outcome(
        variant_set_id=variant_set_id,
        output_dir=output_dir,
        variant_dir=variant_dir,
        event_dir=event_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"sim_outcome_id={result['sim_outcome_id']}")
    typer.echo(f"outcome_dir={result['sim_outcome_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"available_count={manifest['available_count']}")
    typer.echo(f"pending_count={manifest['pending_count']}")
    typer.echo(f"best_variant={manifest['best_variant']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")


@dynamic_v3_backtest_sim_app.command("outcome-report")
def dynamic_v3_backtest_sim_outcome_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest backtest sim outcome。"),
    ] = False,
    sim_outcome_id: Annotated[
        str | None,
        typer.Option("--sim-outcome-id", help="simulation outcome id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation outcome artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
) -> None:
    """展示 TRADING-163 outcome 摘要。"""
    payload = backtest_sim_outcome_report_payload(
        sim_outcome_id=sim_outcome_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"sim_outcome_id={payload['sim_outcome_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"available_count={payload['available_count']}")
    typer.echo(f"pending_count={payload['pending_count']}")
    typer.echo(f"best_variant={payload['best_variant']}")
    typer.echo(f"report_path={payload['backtest_sim_outcome_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backtest-sim-outcome")
def dynamic_v3_validate_backtest_sim_outcome_command(
    sim_outcome_id: Annotated[
        str,
        typer.Option("--sim-outcome-id", help="simulation outcome id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation outcome artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
) -> None:
    """校验 TRADING-163 backtest simulation outcome artifact。"""
    payload = validate_backtest_sim_outcome_artifact(
        sim_outcome_id=sim_outcome_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backtest_sim_outcome_run_command",
    "dynamic_v3_backtest_sim_outcome_report_command",
    "dynamic_v3_validate_backtest_sim_outcome_command",
]
