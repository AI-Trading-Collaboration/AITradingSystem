from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.expanded_allocation_universe import (
    DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_STATE_PORTFOLIO_CANDIDATES_PATH,
    DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    run_expanded_actual_path_rebacktest,
    run_expanded_candidate_failure_matrix,
    run_expanded_universe_owner_review_pack,
    run_expanded_universe_scope_review,
    run_risk_bucket_representatives,
    run_state_portfolio_candidates,
    run_static_frontier_review,
    run_static_simplex_grid,
    run_tqqq_data_quality_blocking_review,
)

console = Console()
expanded_universe_app = typer.Typer(
    help="Expanded QQQ / SGOV / TQQQ allocation research.",
    no_args_is_help=True,
)


def register_expanded_universe_strategy_commands(strategies_app: typer.Typer) -> None:
    strategies_app.add_typer(expanded_universe_app, name="expanded-universe")


@expanded_universe_app.command("scope-review")
def expanded_scope_review_command(
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
) -> None:
    payload = run_expanded_universe_scope_review(config_path=config_path)
    _print_payload("Expanded universe scope", payload)


@expanded_universe_app.command("tqqq-data-quality-review")
def tqqq_data_quality_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_tqqq_data_quality_blocking_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("TQQQ data quality blocking review", payload)


@expanded_universe_app.command("static-simplex-grid")
def static_simplex_grid_command(
    assets: Annotated[
        list[str] | None,
        typer.Option("--assets", help="Asset tickers; repeat or pass QQQ SGOV TQQQ."),
    ] = None,
    step: Annotated[float | None, typer.Option("--step")] = None,
    rebalance: Annotated[str | None, typer.Option("--rebalance")] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output", "--output-root"),
    ] = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_static_simplex_grid(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        assets=assets,
        step=step,
        rebalance=rebalance,
        as_of_date=_parse_optional_date(as_of),
        start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
        end_date=_parse_optional_date(end_date),
    )
    _print_payload("Static simplex grid", payload)


@expanded_universe_app.command("static-frontier-review")
def static_frontier_review_command(
    static_grid_root: Annotated[
        Path, typer.Option("--static-grid-root")
    ] = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
) -> None:
    payload = run_static_frontier_review(
        static_grid_root=static_grid_root,
        config_path=config_path,
    )
    _print_payload("Static frontier review", payload)


@expanded_universe_app.command("risk-bucket-representatives")
def risk_bucket_representatives_command(
    static_grid_root: Annotated[
        Path, typer.Option("--static-grid-root")
    ] = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    output_path: Annotated[
        Path | None, typer.Option("--output")
    ] = None,
) -> None:
    payload = run_risk_bucket_representatives(
        static_grid_root=static_grid_root,
        config_path=config_path,
        output_path=output_path,
    )
    _print_payload("Risk bucket representatives", payload)


@expanded_universe_app.command("state-portfolio-candidates")
def state_portfolio_candidates_command(
    representatives_path: Annotated[
        Path | None, typer.Option("--representatives")
    ] = None,
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    output_path: Annotated[
        Path | None, typer.Option("--output")
    ] = None,
) -> None:
    payload = run_state_portfolio_candidates(
        representatives_path=representatives_path,
        config_path=config_path,
        output_path=output_path,
    )
    _print_payload("State portfolio candidates", payload)


@expanded_universe_app.command("actual-path-rebacktest")
def actual_path_rebacktest_command(
    output_root: Annotated[
        Path,
        typer.Option("--output", "--output-root"),
    ] = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    static_grid_root: Annotated[
        Path, typer.Option("--static-grid-root")
    ] = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    candidates_path: Annotated[
        Path | None, typer.Option("--candidates")
    ] = None,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_expanded_actual_path_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        static_grid_root=static_grid_root,
        candidates_path=candidates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
        start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
        end_date=_parse_optional_date(end_date),
    )
    _print_payload("Expanded actual-path rebacktest", payload)


@expanded_universe_app.command("owner-review-pack")
def owner_review_pack_command(
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    static_grid_root: Annotated[
        Path, typer.Option("--static-grid-root")
    ] = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    actual_path_root: Annotated[
        Path, typer.Option("--actual-path-root")
    ] = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
) -> None:
    payload = run_expanded_universe_owner_review_pack(
        config_path=config_path,
        static_grid_root=static_grid_root,
        actual_path_root=actual_path_root,
    )
    _print_payload("Expanded owner review pack", payload)


@expanded_universe_app.command("candidate-failure-matrix")
def candidate_failure_matrix_command(
    config_path: Annotated[Path, typer.Option("--config")] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    static_grid_root: Annotated[
        Path, typer.Option("--static-grid-root")
    ] = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    actual_path_root: Annotated[
        Path, typer.Option("--actual-path-root")
    ] = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    candidates_path: Annotated[
        Path, typer.Option("--candidates")
    ] = DEFAULT_STATE_PORTFOLIO_CANDIDATES_PATH,
) -> None:
    payload = run_expanded_candidate_failure_matrix(
        config_path=config_path,
        static_grid_root=static_grid_root,
        actual_path_root=actual_path_root,
        candidates_path=candidates_path,
    )
    _print_payload("Expanded candidate failure matrix", payload)


def _print_payload(label: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "APPROVED" in status else "yellow"
    if "BLOCKED" in status or "FAIL" in status:
        style = "red"
    console.print(f"[{style}]{label}: {status}[/{style}]")
    summary = payload.get("summary")
    if isinstance(summary, dict):
        compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:6])
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
    fail_closed_status = status == "FAIL" or (
        "BLOCKED" in status and "PROMOTION_BLOCKED" not in status
    )
    if fail_closed_status:
        raise typer.Exit(code=1)


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("Date must use YYYY-MM-DD.") from exc
