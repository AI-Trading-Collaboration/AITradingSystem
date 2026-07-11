from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    position_advisory_daily_report_payload,
    run_position_advisory_daily,
    validate_position_advisory_daily_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_position_advisory_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_position_advisory_app.command("daily-run")
def dynamic_v3_position_advisory_daily_run_command(
    shadow_monitor_run_id: Annotated[
        str,
        typer.Option("--shadow-monitor-run-id", help="shadow monitor run id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="position advisory config。"),
    ] = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    portfolio_snapshot: Annotated[
        Path | None,
        typer.Option("--portfolio-snapshot", help="optional current portfolio snapshot YAML。"),
    ] = None,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Annotated[
        Path,
        typer.Option("--consensus-drift-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """生成 TRADING-133 daily position advisory。"""
    result = run_position_advisory_daily(
        shadow_monitor_run_id=shadow_monitor_run_id,
        config_path=config_path,
        portfolio_snapshot_path=portfolio_snapshot,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        consensus_drift_dir=consensus_drift_dir,
        output_dir=output_dir,
    )
    actions = result["daily_advisory_actions"]
    typer.echo(f"daily_advisory_id={result['daily_advisory_id']}")
    typer.echo(f"daily_advisory_dir={result['daily_advisory_dir']}")
    typer.echo(f"mode={actions['mode']}")
    typer.echo(f"recommended_action={actions['recommended_action']}")
    typer.echo("owner_approval_required=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_position_advisory_app.command("daily-report")
def dynamic_v3_position_advisory_daily_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest daily advisory。"),
    ] = False,
    daily_advisory_id: Annotated[
        str | None,
        typer.Option("--daily-advisory-id", help="daily advisory id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """展示 TRADING-133 daily position advisory 摘要。"""
    payload = position_advisory_daily_report_payload(
        daily_advisory_id=daily_advisory_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"daily_advisory_id={payload['daily_advisory_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"mode={payload['mode']}")
    typer.echo(f"recommended_action={payload['recommended_action']}")
    typer.echo(f"report_path={payload['daily_position_advisory_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-position-advisory-daily")
def dynamic_v3_validate_position_advisory_daily_command(
    daily_advisory_id: Annotated[
        str,
        typer.Option("--daily-advisory-id", help="daily advisory id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """校验 TRADING-133 daily position advisory artifact。"""
    payload = validate_position_advisory_daily_artifact(
        daily_advisory_id=daily_advisory_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("owner_approval_required=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
