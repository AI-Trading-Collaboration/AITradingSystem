from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_POSITION_ADVISORY_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    position_advisory_report_payload,
    run_position_advisory,
    validate_position_advisory_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_position_advisory_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_position_advisory_app.command("run")
def dynamic_v3_position_advisory_run_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="position advisory config。"),
    ] = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    portfolio_snapshot: Annotated[
        Path | None,
        typer.Option("--portfolio-snapshot", help="optional current portfolio snapshot YAML。"),
    ] = None,
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DIR,
) -> None:
    """生成 TRADING-129 position advisory。"""
    result = run_position_advisory(
        shadow_shortlist_id=shadow_shortlist_id,
        config_path=config_path,
        portfolio_snapshot_path=portfolio_snapshot,
        shadow_shortlist_dir=shadow_shortlist_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"advisory_id={result['advisory_id']}")
    typer.echo(f"advisory_dir={result['advisory_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"position_advisory_status={manifest['position_advisory_status']}")
    typer.echo(f"recommended_action={manifest['recommended_action']}")
    typer.echo("owner_approval_required=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_position_advisory_app.command("report")
def dynamic_v3_position_advisory_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest position advisory pointer。"),
    ] = False,
    advisory_id: Annotated[str | None, typer.Option("--advisory-id", help="advisory id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DIR,
) -> None:
    """展示 TRADING-129 position advisory 摘要。"""
    payload = position_advisory_report_payload(
        advisory_id=advisory_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"advisory_id={payload['advisory_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"position_advisory_status={payload['position_advisory_status']}")
    typer.echo(f"recommended_action={payload['recommended_action']}")
    typer.echo(f"report_path={payload['position_advisory_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-position-advisory")
def dynamic_v3_validate_position_advisory_command(
    advisory_id: Annotated[str, typer.Option("--advisory-id", help="advisory id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DIR,
) -> None:
    """校验 TRADING-129 position advisory artifact。"""
    payload = validate_position_advisory_artifact(advisory_id=advisory_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("owner_approval_required=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
