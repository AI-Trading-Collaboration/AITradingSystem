from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_ROBUSTNESS_DIR,
    DEFAULT_SHADOW_REGISTRY_PATH,
    DEFAULT_SHADOW_REPORT_DIR,
    DEFAULT_SWEEP_OUTPUT_DIR,
    DEFAULT_WALK_FORWARD_DIR,
    DynamicV3ParameterResearchError,
    register_shadow_candidate,
    shadow_list_payload,
    shadow_report_payload,
    validate_shadow_registry,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_shadow_app,
)


@dynamic_v3_shadow_app.command("register")
def dynamic_v3_shadow_register_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    walk_forward_id: Annotated[
        str | None,
        typer.Option(
            "--walk-forward-id",
            help="owner显式选择的walk-forward artifact id；必须与--robustness-id同时提供。",
        ),
    ] = None,
    robustness_id: Annotated[
        str | None,
        typer.Option(
            "--robustness-id",
            help="owner显式选择的robustness artifact id；必须与--walk-forward-id同时提供。",
        ),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    walk_forward_dir: Annotated[
        Path,
        typer.Option("--walk-forward-dir", help="walk-forward artifact root。"),
    ] = DEFAULT_WALK_FORWARD_DIR,
    robustness_dir: Annotated[
        Path,
        typer.Option("--robustness-dir", help="robustness artifact root。"),
    ] = DEFAULT_ROBUSTNESS_DIR,
) -> None:
    """登记 TRADING-098 observe-only shadow candidate。"""
    try:
        payload = register_shadow_candidate(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            walk_forward_id=walk_forward_id,
            robustness_id=robustness_id,
            registry_path=registry_path,
            sweep_output_dir=sweep_output_dir,
            walk_forward_dir=walk_forward_dir,
            robustness_dir=robustness_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"registry_path={registry_path}")
    typer.echo(f"observation_basis_status={payload['candidate']['observation_basis_status']}")
    typer.echo("observe_only=true")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_app.command("list")
def dynamic_v3_shadow_list_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
) -> None:
    """列出 TRADING-098 shadow candidates。"""
    payload = shadow_list_payload(registry_path=registry_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"registry_path={registry_path}")
    for row in payload["candidates"]:
        typer.echo(f"{row.get('candidate_id')} status={row.get('status')}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_app.command("report")
def dynamic_v3_shadow_report_command(
    candidate_id: Annotated[
        str | None,
        typer.Option("--candidate-id", help="candidate id。"),
    ] = None,
    all_candidates: Annotated[
        bool,
        typer.Option("--all/--no-all", help="report all shadow candidates。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow report output root。"),
    ] = DEFAULT_SHADOW_REPORT_DIR,
) -> None:
    """生成 TRADING-098 shadow report。"""
    if not all_candidates and not candidate_id:
        raise typer.BadParameter("--candidate-id or --all is required")
    try:
        payload = shadow_report_payload(
            candidate_id=candidate_id,
            all_candidates=all_candidates,
            registry_path=registry_path,
            output_dir=output_dir,
            write=True,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"report_count={len(payload['reports'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shadow-registry")
def dynamic_v3_validate_shadow_registry_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    walk_forward_dir: Annotated[
        Path,
        typer.Option("--walk-forward-dir", help="walk-forward artifact root。"),
    ] = DEFAULT_WALK_FORWARD_DIR,
    robustness_dir: Annotated[
        Path,
        typer.Option("--robustness-dir", help="robustness artifact root。"),
    ] = DEFAULT_ROBUSTNESS_DIR,
) -> None:
    """校验 TRADING-098 shadow registry。"""
    payload = validate_shadow_registry(
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
        walk_forward_dir=walk_forward_dir,
        robustness_dir=robustness_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
