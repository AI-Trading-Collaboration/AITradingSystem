from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.current_subscription_qualification import (
    DEFAULT_COST_LIQUIDITY_QUALIFICATION_CONFIG_PATH,
    DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
    run_cost_liquidity_model_qualification,
)
from ai_trading_system.data_foundation import (
    DEFAULT_COST_LIQUIDITY_OUTPUT_ROOT,
    audit_cost_liquidity,
    estimate_trading_costs,
)

console = Console()
trading_costs_app = typer.Typer(help="Research-only cost, liquidity, and tradability model。")


@trading_costs_app.command("estimate")
def trading_costs_estimate_command(
    orders: Annotated[
        Path | None,
        typer.Option("--orders", help="Orders artifact JSON/JSONL；省略时使用空订单 baseline。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Cost/liquidity 输出目录。"),
    ] = DEFAULT_COST_LIQUIDITY_OUTPUT_ROOT,
) -> None:
    payload = estimate_trading_costs(orders_path=orders, output_root=output_root)
    _print_payload(payload)


@trading_costs_app.command("audit")
def trading_costs_audit_command(
    universe: Annotated[
        str,
        typer.Option("--universe", help="Universe id。"),
    ] = "data_foundation_minimum",
    date_range: Annotated[
        str,
        typer.Option("--date-range", help="Date range start:end。"),
    ] = "2021-02-22:2021-02-22",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Cost/liquidity 输出目录。"),
    ] = DEFAULT_COST_LIQUIDITY_OUTPUT_ROOT,
) -> None:
    payload = audit_cost_liquidity(
        universe=universe,
        date_range=date_range,
        output_root=output_root,
    )
    _print_payload(payload)


@trading_costs_app.command("qualify-model")
def trading_costs_qualify_model_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="TRADING-747 cost/liquidity qualification config。"),
    ] = DEFAULT_COST_LIQUIDITY_QUALIFICATION_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-747 cost/liquidity qualification 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = run_cost_liquidity_model_qualification(
        config_path=config_path,
        output_root=output_root,
    )
    _print_payload(payload)


def _print_payload(payload: dict[str, object]) -> None:
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if "WARNING" in status else "red"
    console.print(
        f"[{style}]{payload.get('title', payload.get('report_type'))}：{status}[/{style}]"
    )
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in sorted(summary):
            console.print(f"{key}={summary[key]}")
    console.print("production_effect=none；broker_action=none")
