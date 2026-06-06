from __future__ import annotations

import typer

from ai_trading_system.cli_commands import etf_portfolio as etf_cli

features_app = typer.Typer(help="ETF feature store compatibility aliases。", no_args_is_help=True)
regime_app = typer.Typer(help="ETF market regime compatibility aliases。", no_args_is_help=True)
simulation_app = typer.Typer(
    help="ETF simulation ledger compatibility aliases。",
    no_args_is_help=True,
)
report_app = typer.Typer(help="ETF report compatibility aliases。", no_args_is_help=True)
run_app = typer.Typer(help="ETF workflow compatibility aliases。", no_args_is_help=True)
experiments_app = typer.Typer(
    help="ETF experiment registry compatibility aliases。",
    no_args_is_help=True,
)


def register_etf_compatibility_aliases(
    *,
    data_app: typer.Typer,
    portfolio_app: typer.Typer,
    signals_app: typer.Typer,
) -> None:
    """Expose document-style ETF P0 command paths without duplicating logic."""
    data_app.command(
        "ingest",
        help="ETF compatibility alias for `aits etf data ingest`.",
    )(etf_cli.data_ingest_command)
    data_app.command(
        "validate",
        help="ETF compatibility alias for `aits etf data validate`.",
    )(etf_cli.data_validate_command)
    features_app.command(
        "build",
        help="ETF compatibility alias for `aits etf features build`.",
    )(etf_cli.features_build_command)
    signals_app.command(
        "generate",
        help="ETF compatibility alias for `aits etf signals generate`.",
    )(etf_cli.signals_generate_command)
    regime_app.command(
        "generate",
        help="ETF compatibility alias for `aits etf regime generate`.",
    )(etf_cli.regime_generate_command)
    portfolio_app.command(
        "allocate",
        help="ETF compatibility alias for `aits etf portfolio allocate`.",
    )(etf_cli.portfolio_allocate_command)
    simulation_app.command(
        "record",
        help="ETF compatibility alias for `aits etf simulation record`.",
    )(etf_cli.simulation_record_command)
    simulation_app.command(
        "evaluate",
        help="ETF compatibility alias for `aits etf simulation evaluate`.",
    )(etf_cli.simulation_evaluate_command)
    simulation_app.command(
        "report",
        help="ETF compatibility alias for `aits etf simulation report`.",
    )(etf_cli.simulation_report_command)
    report_app.command(
        "daily",
        help="ETF compatibility alias for `aits etf report daily`.",
    )(etf_cli.report_daily_command)
    run_app.command(
        "daily",
        help="ETF compatibility alias for `aits etf run daily`.",
    )(etf_cli.run_daily_command)
    experiments_app.command(
        "register",
        help="ETF compatibility alias for `aits etf experiments register`.",
    )(etf_cli.experiments_register_command)
    experiments_app.command(
        "run",
        help="ETF compatibility alias for `aits etf experiments run`.",
    )(etf_cli.experiments_run_command)
    experiments_app.command(
        "compare",
        help="ETF compatibility alias for `aits etf experiments compare`.",
    )(etf_cli.experiments_compare_command)
