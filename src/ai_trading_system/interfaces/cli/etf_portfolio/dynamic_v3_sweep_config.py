from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DynamicV3ParameterResearchError,
    build_sweep_config_validation,
    preview_sweep_candidates,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_v3_sweep_config_app


@dynamic_v3_sweep_config_app.command("validate")
def dynamic_v3_sweep_config_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
) -> None:
    """校验 TRADING-093 parameter sweep config contract。"""
    payload = build_sweep_config_validation(config_path=config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"config_path={config_path}")
    typer.echo(f"candidate_preview_count={payload.get('candidate_preview_count')}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sweep_config_app.command("preview")
def dynamic_v3_sweep_config_preview_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    limit: Annotated[int, typer.Option("--limit", help="preview candidate count。")] = 20,
) -> None:
    """预览 TRADING-093 parameter sweep candidates。"""
    try:
        payload = preview_sweep_candidates(config_path=config_path, limit=limit)
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"preview_count={payload['preview_count']}")
    for row in payload["candidates"]:
        typer.echo(f"{row['candidate_id']} {json.dumps(row['parameters'], sort_keys=True)}")
    typer.echo("production_candidate_generated=false")
