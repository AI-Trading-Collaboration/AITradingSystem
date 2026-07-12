from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_HISTORICAL_REPLAY_DIR,
    DEFAULT_REPLAY_INVENTORY_DIR,
    historical_replay_report_payload,
    run_historical_replay,
    validate_historical_replay_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_historical_replay_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_historical_replay_app.command("run")
def dynamic_v3_historical_replay_run_command(
    inventory_id: Annotated[str, typer.Option("--inventory-id", help="inventory id。")],
    include_pit_warning: Annotated[
        bool,
        typer.Option("--include-pit-warning", help="允许 PIT_WARNING 进入 replay。"),
    ] = False,
    inventory_dir: Annotated[
        Path,
        typer.Option("--inventory-dir", help="replay inventory artifact root。"),
    ] = DEFAULT_REPLAY_INVENTORY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="historical replay artifact root。"),
    ] = DEFAULT_HISTORICAL_REPLAY_DIR,
) -> None:
    """运行 TRADING-142 historical advisory replay。"""
    result = run_historical_replay(
        inventory_id=inventory_id,
        include_pit_warning=include_pit_warning,
        inventory_dir=inventory_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    summary = result["action_summary"]
    typer.echo(f"replay_id={result['replay_id']}")
    typer.echo(f"replay_dir={result['replay_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"replay_event_count={manifest['replay_event_count']}")
    typer.echo(f"skipped_count={manifest['skipped_count']}")
    typer.echo(f"generated_variants={','.join(manifest['generated_variants'])}")
    typer.echo(f"broker_action_present={summary['broker_action_present']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_historical_replay_app.command("report")
def dynamic_v3_historical_replay_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest historical replay。"),
    ] = False,
    replay_id: Annotated[str | None, typer.Option("--replay-id", help="replay id。")]=None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="historical replay artifact root。"),
    ] = DEFAULT_HISTORICAL_REPLAY_DIR,
) -> None:
    """展示 TRADING-142 historical replay 摘要。"""
    payload = historical_replay_report_payload(
        replay_id=replay_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload.get("replay_action_summary", {})
    typer.echo(f"replay_id={payload['replay_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"replay_event_count={payload['replay_event_count']}")
    typer.echo(f"skipped_count={payload['skipped_count']}")
    typer.echo(f"broker_action_present={summary.get('broker_action_present')}")
    typer.echo(f"report_path={payload['historical_replay_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-historical-replay")
def dynamic_v3_validate_historical_replay_command(
    replay_id: Annotated[str, typer.Option("--replay-id", help="replay id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="historical replay artifact root。"),
    ] = DEFAULT_HISTORICAL_REPLAY_DIR,
) -> None:
    """校验 TRADING-142 historical replay artifact。"""
    payload = validate_historical_replay_artifact(replay_id=replay_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_historical_replay_run_command",
    "dynamic_v3_historical_replay_report_command",
    "dynamic_v3_validate_historical_replay_command",
]
