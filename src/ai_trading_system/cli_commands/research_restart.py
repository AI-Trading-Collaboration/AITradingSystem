from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.research_restart import (
    DEFAULT_COST_POLICY_PATH,
    DEFAULT_EXECUTION_POLICY_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_PRIMARY_WINDOW_POLICY_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_RESTART_OUTPUT_ROOT,
    DEFAULT_RESTART_POLICY_PATH,
    DEFAULT_SECONDARY_PRICES_PATH,
    DEFAULT_WINDOW_REGISTRY_PATH,
    ResearchRestartError,
    run_research_restart_preflight,
    validate_research_restart_preflight,
)


def register_research_restart_commands(app: typer.Typer) -> None:
    # The restart-decision and clean-selection gate commands were removed with their dormant modules
    # (GOV-008 P4 block 2); the R0 preflight stays with research_restart.
    app.command("strategy-restart-preflight")(strategy_restart_preflight_command)
    app.command("validate-strategy-restart-preflight")(validate_strategy_restart_preflight_command)


def strategy_restart_preflight_command(
    source_sweep_dir: Annotated[
        Path, typer.Option("--source-sweep-dir", help="TRADING-096/097 source sweep目录。")
    ],
    policy_path: Annotated[
        Path, typer.Option("--policy-path", help="R0～R2 restart policy。")
    ] = DEFAULT_RESTART_POLICY_PATH,
    primary_window_policy_path: Annotated[
        Path, typer.Option("--primary-window-policy", help="Primary research window policy。")
    ] = DEFAULT_PRIMARY_WINDOW_POLICY_PATH,
    window_registry_path: Annotated[
        Path, typer.Option("--window-registry", help="Research window registry。")
    ] = DEFAULT_WINDOW_REGISTRY_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="Primary prices cache。")
    ] = DEFAULT_PRICES_PATH,
    secondary_prices_path: Annotated[
        Path, typer.Option("--secondary-prices-path", help="Secondary prices cache。")
    ] = DEFAULT_SECONDARY_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="Rates cache。")
    ] = DEFAULT_RATES_PATH,
    download_manifest_path: Annotated[
        Path | None, typer.Option("--download-manifest", help="Download manifest。")
    ] = None,
    cost_policy_path: Annotated[
        Path, typer.Option("--cost-policy", help="Transaction cost policy。")
    ] = DEFAULT_COST_POLICY_PATH,
    execution_policy_path: Annotated[
        Path, typer.Option("--execution-policy", help="Execution lag policy。")
    ] = DEFAULT_EXECUTION_POLICY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="R0 artifact输出目录。")
    ] = DEFAULT_RESTART_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of", help="DQ as-of date。")] = None,
) -> None:
    try:
        payload = run_research_restart_preflight(
            source_sweep_dir=source_sweep_dir,
            policy_path=policy_path,
            primary_window_policy_path=primary_window_policy_path,
            window_registry_path=window_registry_path,
            prices_path=prices_path,
            secondary_prices_path=secondary_prices_path,
            rates_path=rates_path,
            download_manifest_path=download_manifest_path,
            cost_policy_path=cost_policy_path,
            execution_policy_path=execution_policy_path,
            output_root=output_root,
            as_of=None if as_of is None else date.fromisoformat(as_of),
        )
    except (ResearchRestartError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_payload(payload)
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def validate_strategy_restart_preflight_command(
    artifact_path: Annotated[
        Path, typer.Option("--artifact-path", help="R0 preflight JSON。")
    ] = DEFAULT_RESTART_OUTPUT_ROOT
    / "strategy_research_restart_preflight.json",
) -> None:
    try:
        payload = validate_research_restart_preflight(artifact_path=artifact_path)
    except (ResearchRestartError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_payload(payload)
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def _print_payload(payload: MappingLike) -> None:
    typer.echo(f"status={payload.get('status')}")
    if "research_execution_unblocked" in payload:
        typer.echo(
            f"research_execution_unblocked={str(payload.get('research_execution_unblocked')).lower()}"
        )
    typer.echo(f"failed_check_count={payload.get('failed_check_count', 0)}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


MappingLike = dict[str, Any]
