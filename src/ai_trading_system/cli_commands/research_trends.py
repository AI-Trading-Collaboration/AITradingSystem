from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.first_layer_policy_calibration import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_PROBE_REGISTRY_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    DEFAULT_SCOPE_CONFIG_PATH,
    DEFAULT_SCORE_POLICY_PATH,
    DEFAULT_SCORECARD_CONFIG_PATH,
    run_first_layer_policy_aware_calibration_pack,
)
from ai_trading_system.first_layer_up_state_learning import (
    DEFAULT_HIERARCHICAL_CONFIG_PATH,
    DEFAULT_THRESHOLD_POLICY_PATH,
    run_first_layer_up_state_learning_repair_pack,
)

console = Console()
trends_app = typer.Typer(
    help="Policy-aware first-layer trend calibration research.",
    no_args_is_help=True,
)


@trends_app.command("full-pack")
def first_layer_policy_aware_full_pack_command(
    scope_config_path: Annotated[Path, typer.Option("--scope-config")] = DEFAULT_SCOPE_CONFIG_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Annotated[Path, typer.Option("--score-policy")] = DEFAULT_SCORE_POLICY_PATH,
    scorecard_config_path: Annotated[
        Path, typer.Option("--scorecard-config")
    ] = DEFAULT_SCORECARD_CONFIG_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_first_layer_policy_aware_calibration_pack(
        scope_config_path=scope_config_path,
        probe_registry_path=probe_registry_path,
        score_policy_path=score_policy_path,
        scorecard_config_path=scorecard_config_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("First-layer policy-aware calibration", payload)


@trends_app.command("up-state-repair")
def first_layer_up_state_repair_command(
    scope_config_path: Annotated[Path, typer.Option("--scope-config")] = DEFAULT_SCOPE_CONFIG_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Annotated[Path, typer.Option("--score-policy")] = DEFAULT_SCORE_POLICY_PATH,
    scorecard_config_path: Annotated[
        Path, typer.Option("--scorecard-config")
    ] = DEFAULT_SCORECARD_CONFIG_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_THRESHOLD_POLICY_PATH,
    hierarchical_config_path: Annotated[
        Path, typer.Option("--hierarchical-config")
    ] = DEFAULT_HIERARCHICAL_CONFIG_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    refresh_prerequisites: Annotated[
        bool, typer.Option("--refresh-prerequisites/--no-refresh-prerequisites")
    ] = True,
) -> None:
    payload = run_first_layer_up_state_learning_repair_pack(
        scope_config_path=scope_config_path,
        probe_registry_path=probe_registry_path,
        score_policy_path=score_policy_path,
        scorecard_config_path=scorecard_config_path,
        threshold_policy_path=threshold_policy_path,
        hierarchical_config_path=hierarchical_config_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        refresh_prerequisites=refresh_prerequisites,
    )
    _print_payload("First-layer up-state learning repair", payload)


def _print_payload(label: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "CANDIDATE" in status else "yellow"
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
