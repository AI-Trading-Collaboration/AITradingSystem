from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_ROBUSTNESS_DIR,
    DEFAULT_SWEEP_OUTPUT_DIR,
    DEFAULT_WALK_FORWARD_DIR,
    DynamicV3ParameterResearchError,
    robustness_report_payload,
    run_robustness_diagnostics,
    run_walk_forward_validation,
    validate_robustness_artifact,
    validate_walk_forward_artifact,
    walk_forward_report_payload,
)
from ai_trading_system.etf_portfolio.dynamic_v3_r1_evidence import (
    DEFAULT_R0_PREFLIGHT_PATH,
    DEFAULT_R1_ROBUSTNESS_DIR,
    DEFAULT_R1_WALK_FORWARD_DIR,
    DynamicV3R1EvidenceError,
    run_r1_robustness_evidence,
    run_r1_walk_forward_evidence,
    validate_r1_robustness_evidence,
    validate_r1_walk_forward_evidence,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_robustness_app,
    dynamic_v3_walk_forward_app,
)
from ai_trading_system.research_restart import (
    DEFAULT_RESTART_POLICY_PATH,
    ResearchRestartError,
)


@dynamic_v3_walk_forward_app.command("run")
def dynamic_v3_walk_forward_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    top_n: Annotated[int, typer.Option("--top-n", help="top candidate count。")] = 20,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward artifact root。"),
    ] = DEFAULT_WALK_FORWARD_DIR,
) -> None:
    """运行 TRADING-096 walk-forward / OOS validation。"""
    try:
        result = run_walk_forward_validation(
            sweep_id=sweep_id,
            top_n=top_n,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"walk_forward_id={result['walk_forward_id']}")
    typer.echo(f"walk_forward_dir={result['walk_forward_dir']}")
    typer.echo(f"status={result['report']['status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_walk_forward_app.command("report")
def dynamic_v3_walk_forward_report_command(
    walk_forward_id: Annotated[
        str,
        typer.Option("--walk-forward-id", help="walk-forward id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward artifact root。"),
    ] = DEFAULT_WALK_FORWARD_DIR,
) -> None:
    """展示 TRADING-096 walk-forward report。"""
    payload = walk_forward_report_payload(
        walk_forward_id=walk_forward_id,
        output_dir=output_dir,
    )
    typer.echo(f"walk_forward_id={walk_forward_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-walk-forward")
def dynamic_v3_validate_walk_forward_command(
    walk_forward_id: Annotated[
        str,
        typer.Option("--walk-forward-id", help="walk-forward id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="walk-forward artifact root。"),
    ] = DEFAULT_WALK_FORWARD_DIR,
) -> None:
    """校验 TRADING-096 walk-forward artifacts。"""
    payload = validate_walk_forward_artifact(
        walk_forward_id=walk_forward_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_robustness_app.command("run")
def dynamic_v3_robustness_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness artifact root。"),
    ] = DEFAULT_ROBUSTNESS_DIR,
) -> None:
    """运行 TRADING-097 robustness diagnostics。"""
    try:
        result = run_robustness_diagnostics(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = result["report"]
    typer.echo(f"robustness_id={result['robustness_id']}")
    typer.echo(f"robustness_dir={result['robustness_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"overfit_status={report['overfit_status']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_robustness_app.command("report")
def dynamic_v3_robustness_report_command(
    robustness_id: Annotated[
        str,
        typer.Option("--robustness-id", help="robustness id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness artifact root。"),
    ] = DEFAULT_ROBUSTNESS_DIR,
) -> None:
    """展示 TRADING-097 robustness report。"""
    payload = robustness_report_payload(
        robustness_id=robustness_id,
        output_dir=output_dir,
    )
    typer.echo(f"robustness_id={robustness_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-robustness")
def dynamic_v3_validate_robustness_command(
    robustness_id: Annotated[
        str,
        typer.Option("--robustness-id", help="robustness id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="robustness artifact root。"),
    ] = DEFAULT_ROBUSTNESS_DIR,
) -> None:
    """校验 TRADING-097 robustness artifacts。"""
    payload = validate_robustness_artifact(
        robustness_id=robustness_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_walk_forward_app.command("r1-run")
def dynamic_v3_r1_walk_forward_run_command(
    source_sweep_dir: Annotated[
        Path, typer.Option("--source-sweep-dir", help="R1 source sweep目录。")
    ],
    prices_path: Annotated[Path, typer.Option("--prices-path", help="R0冻结的prices cache。")],
    top_n: Annotated[int, typer.Option("--top-n", help="source leaderboard top-N。")] = 20,
    restart_preflight_path: Annotated[
        Path, typer.Option("--restart-preflight", help="R0 preflight JSON。")
    ] = DEFAULT_R0_PREFLIGHT_PATH,
    restart_policy_path: Annotated[
        Path, typer.Option("--restart-policy", help="R0～R2 policy。")
    ] = DEFAULT_RESTART_POLICY_PATH,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="R1 walk-forward artifact root。")
    ] = DEFAULT_R1_WALK_FORWARD_DIR,
    workers: Annotated[int, typer.Option("--workers", help="受控并行evaluator数。")] = 4,
) -> None:
    try:
        result = run_r1_walk_forward_evidence(
            source_sweep_dir=source_sweep_dir,
            top_n=top_n,
            restart_preflight_path=restart_preflight_path,
            restart_policy_path=restart_policy_path,
            prices_path=prices_path,
            output_dir=output_dir,
            workers=workers,
        )
    except (DynamicV3R1EvidenceError, ResearchRestartError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"walk_forward_id={result['walk_forward_id']}")
    typer.echo(f"walk_forward_dir={result['walk_forward_dir']}")
    typer.echo(f"status={result['report']['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@dynamic_v3_rescue_app.command("validate-r1-walk-forward")
def dynamic_v3_validate_r1_walk_forward_command(
    walk_forward_id: Annotated[str, typer.Option("--walk-forward-id", help="R1 walk-forward id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="R1 walk-forward artifact root。")
    ] = DEFAULT_R1_WALK_FORWARD_DIR,
) -> None:
    payload = validate_r1_walk_forward_evidence(
        walk_forward_id=walk_forward_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_robustness_app.command("r1-run")
def dynamic_v3_r1_robustness_run_command(
    source_sweep_dir: Annotated[
        Path, typer.Option("--source-sweep-dir", help="R1 source sweep目录。")
    ],
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="source candidate id。")],
    prices_path: Annotated[Path, typer.Option("--prices-path", help="R0冻结的prices cache。")],
    restart_preflight_path: Annotated[
        Path, typer.Option("--restart-preflight", help="R0 preflight JSON。")
    ] = DEFAULT_R0_PREFLIGHT_PATH,
    restart_policy_path: Annotated[
        Path, typer.Option("--restart-policy", help="R0～R2 policy。")
    ] = DEFAULT_RESTART_POLICY_PATH,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="R1 robustness artifact root。")
    ] = DEFAULT_R1_ROBUSTNESS_DIR,
) -> None:
    try:
        result = run_r1_robustness_evidence(
            source_sweep_dir=source_sweep_dir,
            candidate_id=candidate_id,
            restart_preflight_path=restart_preflight_path,
            restart_policy_path=restart_policy_path,
            prices_path=prices_path,
            output_dir=output_dir,
        )
    except (DynamicV3R1EvidenceError, ResearchRestartError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"robustness_id={result['robustness_id']}")
    typer.echo(f"robustness_dir={result['robustness_dir']}")
    typer.echo(f"status={result['report']['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@dynamic_v3_rescue_app.command("validate-r1-robustness")
def dynamic_v3_validate_r1_robustness_command(
    robustness_id: Annotated[str, typer.Option("--robustness-id", help="R1 robustness id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="R1 robustness artifact root。")
    ] = DEFAULT_R1_ROBUSTNESS_DIR,
) -> None:
    payload = validate_r1_robustness_evidence(
        robustness_id=robustness_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
