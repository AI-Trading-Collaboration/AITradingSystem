from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_OVERFIT_DIR,
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DEFAULT_SWEEP_OUTPUT_DIR,
    DEFAULT_WALK_FORWARD_SELECTION_DIR,
    DynamicV3ParameterResearchError,
    overfit_report_payload,
    run_overfit_review,
    run_walk_forward_selection,
    validate_overfit_artifact,
    validate_walk_forward_selection_artifact,
    walk_forward_selection_report_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_overfit_app,
    dynamic_v3_rescue_app,
    dynamic_v3_walk_forward_app,
)


@dynamic_v3_walk_forward_app.command("select-run")
def dynamic_v3_walk_forward_select_run_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    profile: Annotated[str, typer.Option("--profile", help="profile name。")] = "small_real",
    sweep_id: Annotated[str | None, typer.Option("--sweep-id", help="source sweep id。")] = None,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward selection artifact root。"),
    ] = DEFAULT_WALK_FORWARD_SELECTION_DIR,
) -> None:
    """运行 TRADING-106 true walk-forward selection。"""
    try:
        result = run_walk_forward_selection(
            config_path=config_path,
            profile=profile,
            sweep_id=sweep_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"wf_selection_id={result['wf_selection_id']}")
    typer.echo(f"wf_selection_dir={result['wf_selection_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_walk_forward_app.command("selection-report")
def dynamic_v3_walk_forward_selection_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest walk-forward selection。"),
    ] = False,
    wf_selection_id: Annotated[
        str | None,
        typer.Option("--wf-selection-id", help="walk-forward selection id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward selection artifact root。"),
    ] = DEFAULT_WALK_FORWARD_SELECTION_DIR,
) -> None:
    """展示 TRADING-106 walk-forward selection report。"""
    payload = walk_forward_selection_report_payload(
        wf_selection_id=wf_selection_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"wf_selection_id={payload['wf_selection_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-walk-forward-selection")
def dynamic_v3_validate_walk_forward_selection_command(
    wf_selection_id: Annotated[
        str,
        typer.Option("--wf-selection-id", help="walk-forward selection id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward selection artifact root。"),
    ] = DEFAULT_WALK_FORWARD_SELECTION_DIR,
) -> None:
    """校验 TRADING-106 walk-forward selection artifacts。"""
    payload = validate_walk_forward_selection_artifact(
        wf_selection_id=wf_selection_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_overfit_app.command("run")
def dynamic_v3_overfit_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overfit artifact root。"),
    ] = DEFAULT_OVERFIT_DIR,
) -> None:
    """运行 TRADING-107 overfit risk review。"""
    try:
        result = run_overfit_review(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"overfit_id={result['overfit_id']}")
    typer.echo(f"overfit_dir={result['overfit_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"overfit_status={report['overfit_status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_overfit_app.command("report")
def dynamic_v3_overfit_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest overfit pointer。"),
    ] = False,
    overfit_id: Annotated[str | None, typer.Option("--overfit-id", help="overfit id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overfit artifact root。"),
    ] = DEFAULT_OVERFIT_DIR,
) -> None:
    """展示 TRADING-107 overfit report。"""
    payload = overfit_report_payload(
        overfit_id=overfit_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"overfit_id={payload['overfit_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"overfit_status={payload['overfit_status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-overfit")
def dynamic_v3_validate_overfit_command(
    overfit_id: Annotated[str, typer.Option("--overfit-id", help="overfit id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overfit artifact root。"),
    ] = DEFAULT_OVERFIT_DIR,
) -> None:
    """校验 TRADING-107 overfit artifacts。"""
    payload = validate_overfit_artifact(overfit_id=overfit_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
