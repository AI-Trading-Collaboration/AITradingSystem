from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    DEFAULT_SWEEP_OUTPUT_DIR,
    DynamicV3ParameterResearchError,
    candidate_report_payload,
    run_candidate_attribution,
    validate_candidate_attribution_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_candidate_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_candidate_app.command("report")
def dynamic_v3_candidate_report_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output", "--output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """生成并展示 TRADING-095 candidate report。"""
    try:
        payload = candidate_report_payload(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            output_dir=output_dir,
            write=True,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"source_sweep_id={sweep_id}")
    typer.echo(f"evaluator_mode={payload.get('evaluator_mode')}")
    typer.echo(f"gate={payload['hard_gate_status']}")
    typer.echo(f"score={payload['score']}")
    typer.echo(
        "candidate_report="
        f"{output_dir / sweep_id / 'candidates' / candidate_id / 'candidate_report.json'}"
    )
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_app.command("attribution")
def dynamic_v3_candidate_attribution_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate attribution artifact root。"),
    ] = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
) -> None:
    """生成 TRADING-105 candidate attribution report。"""
    try:
        result = run_candidate_attribution(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"attribution_dir={result['attribution_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"explainability_status={report['explainability_status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-candidate-attribution")
def dynamic_v3_validate_candidate_attribution_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate attribution artifact root。"),
    ] = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
) -> None:
    """校验 TRADING-105 candidate attribution artifacts。"""
    payload = validate_candidate_attribution_artifact(
        candidate_id=candidate_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
