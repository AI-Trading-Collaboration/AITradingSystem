from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_report_payload,
    run_consensus_drift,
    validate_consensus_drift_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_consensus_drift_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_consensus_drift_app.command("run")
def dynamic_v3_consensus_drift_run_command(
    shadow_monitor_run_id: Annotated[
        str,
        typer.Option("--shadow-monitor-run-id", help="shadow monitor run id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="position advisory config。"),
    ] = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
) -> None:
    """生成 TRADING-134 consensus drift artifact。"""
    result = run_consensus_drift(
        shadow_monitor_run_id=shadow_monitor_run_id,
        config_path=config_path,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        output_dir=output_dir,
    )
    summary = result["summary"]
    typer.echo(f"drift_id={result['drift_id']}")
    typer.echo(f"drift_dir={result['drift_dir']}")
    typer.echo(f"disagreement_status={summary['disagreement_status']}")
    typer.echo(f"position_advisory_implication={summary['position_advisory_implication']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_consensus_drift_app.command("report")
def dynamic_v3_consensus_drift_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest consensus drift。"),
    ] = False,
    drift_id: Annotated[str | None, typer.Option("--drift-id", help="drift id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
) -> None:
    """展示 TRADING-134 consensus drift 摘要。"""
    payload = consensus_drift_report_payload(
        drift_id=drift_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = mapping_obj(payload.get("consensus_drift_summary"))
    typer.echo(f"drift_id={payload['drift_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"disagreement_status={summary.get('disagreement_status')}")
    typer.echo(f"position_advisory_implication={summary.get('position_advisory_implication')}")
    typer.echo(f"report_path={payload['consensus_drift_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-consensus-drift")
def dynamic_v3_validate_consensus_drift_command(
    drift_id: Annotated[str, typer.Option("--drift-id", help="drift id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
) -> None:
    """校验 TRADING-134 consensus drift artifact。"""
    payload = validate_consensus_drift_artifact(drift_id=drift_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
