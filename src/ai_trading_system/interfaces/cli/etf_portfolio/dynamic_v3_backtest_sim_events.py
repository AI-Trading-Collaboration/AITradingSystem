from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_CONFIG_PATH,
    DEFAULT_BACKTEST_SIM_EVENT_DIR,
    backtest_sim_event_report_payload,
    generate_backtest_sim_events,
    validate_backtest_sim_events_artifact,
    validate_backtest_simulation_config,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_backtest_sim_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_backtest_sim_app.command("config-validate")
def dynamic_v3_backtest_sim_config_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="backtest simulation config。"),
    ] = DEFAULT_BACKTEST_SIM_CONFIG_PATH,
) -> None:
    """校验 TRADING-161 backtest simulation config。"""
    payload = validate_backtest_simulation_config(config_path=config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("outcome_mode=BACKTEST_SIMULATION")
    typer.echo("pit_safety_status=SIMULATION_NOT_PIT")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_backtest_sim_app.command("event-generate")
def dynamic_v3_backtest_sim_event_generate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="backtest simulation config。"),
    ] = DEFAULT_BACKTEST_SIM_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation event artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_EVENT_DIR,
) -> None:
    """生成 TRADING-161 simulated advisory events。"""
    result = generate_backtest_sim_events(config_path=config_path, output_dir=output_dir)
    manifest = result["manifest"]
    typer.echo(f"event_set_id={result['event_set_id']}")
    typer.echo(f"event_dir={result['event_set_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"event_count={manifest['event_count']}")
    typer.echo(f"ready_count={manifest['ready_count']}")
    typer.echo(f"insufficient_data_count={manifest['insufficient_data_count']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo("outcome_mode=BACKTEST_SIMULATION")
    typer.echo("pit_safety_status=SIMULATION_NOT_PIT")
    typer.echo("production_effect=none")


@dynamic_v3_backtest_sim_app.command("event-report")
def dynamic_v3_backtest_sim_event_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest backtest sim events。"),
    ] = False,
    event_set_id: Annotated[
        str | None,
        typer.Option("--event-set-id", help="event set id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation event artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_EVENT_DIR,
) -> None:
    """展示 TRADING-161 event generation 摘要。"""
    payload = backtest_sim_event_report_payload(
        event_set_id=event_set_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"event_set_id={payload['event_set_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"event_count={payload['event_count']}")
    typer.echo(f"ready_count={payload['ready_count']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['event_generation_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-backtest-sim-events")
def dynamic_v3_validate_backtest_sim_events_command(
    event_set_id: Annotated[str, typer.Option("--event-set-id", help="event set id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="backtest simulation event artifact root。"),
    ] = DEFAULT_BACKTEST_SIM_EVENT_DIR,
) -> None:
    """校验 TRADING-161 backtest simulation event artifact。"""
    payload = validate_backtest_sim_events_artifact(
        event_set_id=event_set_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("outcome_mode=BACKTEST_SIMULATION")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_backtest_sim_config_validate_command",
    "dynamic_v3_backtest_sim_event_generate_command",
    "dynamic_v3_backtest_sim_event_report_command",
    "dynamic_v3_validate_backtest_sim_events_command",
]
