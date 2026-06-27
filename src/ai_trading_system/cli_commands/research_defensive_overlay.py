from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.defensive_overlay_gate import (
    DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    DEFAULT_DEFENSIVE_OVERLAY_CONFIG_PATH,
    DEFAULT_DEFENSIVE_OVERLAY_OUTPUT_ROOT,
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_FAILURE_MATRIX_CSV_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_OVERLAY_METRICS_CSV_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    run_defensive_overlay_research_pack,
)

console = Console()
defensive_overlay_app = typer.Typer(
    help="Defensive overlay no-survivor diagnosis research.",
    no_args_is_help=True,
)


def register_defensive_overlay_strategy_commands(strategies_app: typer.Typer) -> None:
    strategies_app.add_typer(defensive_overlay_app, name="defensive-overlay")


@defensive_overlay_app.command("full-pack")
def defensive_overlay_full_pack_command(
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_DEFENSIVE_OVERLAY_CONFIG_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    failure_matrix_path: Annotated[
        Path, typer.Option("--failure-matrix")
    ] = DEFAULT_FAILURE_MATRIX_CSV_PATH,
    actual_path_root: Annotated[
        Path, typer.Option("--actual-path-root")
    ] = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_DEFENSIVE_OVERLAY_OUTPUT_ROOT,
    overlay_metrics_csv_path: Annotated[
        Path, typer.Option("--overlay-metrics")
    ] = DEFAULT_OVERLAY_METRICS_CSV_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_defensive_overlay_research_pack(
        config_path=config_path,
        expanded_config_path=expanded_config_path,
        failure_matrix_path=failure_matrix_path,
        actual_path_root=actual_path_root,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        overlay_metrics_csv_path=overlay_metrics_csv_path,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Defensive overlay full pack", payload)


def _print_payload(label: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "APPROVED" in status else "yellow"
    if "BLOCKED" in status and "PROMOTION_BLOCKED" not in status:
        style = "red"
    console.print(f"[{style}]{label}: {status}[/{style}]")
    summary = payload.get("summary")
    if isinstance(summary, dict):
        compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:8])
        if compact:
            console.print(compact)
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        for key, value in paths.items():
            console.print(f"{key}={value}")
    for field, expected in (
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("dynamic_promotion_status", "BLOCKED"),
    ):
        console.print(f"{field}={payload.get(field, expected)}")
    if "BLOCKED" in status and "PROMOTION_BLOCKED" not in status:
        raise typer.Exit(code=1)


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("Date must use YYYY-MM-DD.") from exc
