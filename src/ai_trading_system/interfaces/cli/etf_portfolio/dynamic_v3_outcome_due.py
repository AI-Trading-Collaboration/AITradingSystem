from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_OUTCOME_DUE_DIR,
    outcome_due_report_payload,
    outcome_due_update_ready,
    run_outcome_due_scan,
    validate_outcome_due_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_PAPER_PORTFOLIO_DIR,
    DEFAULT_RATES_CACHE_PATH,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_outcome_due_app,
    dynamic_v3_rescue_app,
)


def _parse_outcome_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD") from exc


@dynamic_v3_outcome_due_app.command("scan")
def dynamic_v3_outcome_due_scan_command(
    as_of: Annotated[str, typer.Option("--as-of", help="扫描到期窗口的 as-of 日期。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome due artifact root。")
    ] = DEFAULT_OUTCOME_DUE_DIR,
    advisory_outcome_dir: Annotated[
        Path, typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。")
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="cached ETF price path。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="cached rates path。")
    ] = DEFAULT_RATES_CACHE_PATH,
) -> None:
    """扫描 forward advisory outcome 已到期窗口。"""
    result = run_outcome_due_scan(
        as_of=_parse_outcome_date(as_of, "--as-of"),
        output_dir=output_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    summary = result["pending_window_summary"]
    typer.echo(f"due_id={result['due_id']}")
    typer.echo(f"due_dir={result['due_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"total_pending_windows={summary['total_pending_windows']}")
    typer.echo(f"due_windows={summary['due_windows']}")
    typer.echo(f"update_ready_count={summary['update_ready_count']}")
    typer.echo(f"price_missing_count={summary['price_missing_windows']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_outcome_due_app.command("report")
def dynamic_v3_outcome_due_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest outcome due scan。")
    ] = False,
    due_id: Annotated[str | None, typer.Option("--due-id", help="due id。")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome due artifact root。")
    ] = DEFAULT_OUTCOME_DUE_DIR,
) -> None:
    """展示 outcome due 摘要。"""
    payload = outcome_due_report_payload(due_id=due_id, latest=latest, output_dir=output_dir)
    summary = payload["pending_window_summary"]
    typer.echo(f"due_id={payload['due_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_pending_windows={summary['total_pending_windows']}")
    typer.echo(f"due_windows={summary['due_windows']}")
    typer.echo(f"update_ready_count={summary['update_ready_count']}")
    typer.echo(f"price_missing_count={summary['price_missing_windows']}")
    typer.echo(f"report_path={payload['outcome_due_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_outcome_due_app.command("update-ready")
def dynamic_v3_outcome_due_update_ready_command(
    due_id: Annotated[str, typer.Option("--due-id", help="due id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome due artifact root。")
    ] = DEFAULT_OUTCOME_DUE_DIR,
    advisory_outcome_dir: Annotated[
        Path, typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。")
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    paper_portfolio_dir: Annotated[
        Path, typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。")
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="cached ETF price path。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="cached rates path。")
    ] = DEFAULT_RATES_CACHE_PATH,
) -> None:
    """更新 outcome due scan 中 can_update=true 的 outcome。"""
    result = outcome_due_update_ready(
        due_id=due_id,
        output_dir=output_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    execution = result["execution"]
    typer.echo(f"due_id={due_id}")
    typer.echo(f"ready_window_count={execution['ready_window_count']}")
    typer.echo(f"updated_outcome_count={execution['updated_outcome_count']}")
    typer.echo("not_due_windows_updated=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-outcome-due")
def dynamic_v3_validate_outcome_due_command(
    due_id: Annotated[str, typer.Option("--due-id", help="due id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome due artifact root。")
    ] = DEFAULT_OUTCOME_DUE_DIR,
) -> None:
    """校验 TRADING-151 outcome due artifact。"""
    payload = validate_outcome_due_artifact(due_id=due_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_outcome_due_scan_command",
    "dynamic_v3_outcome_due_report_command",
    "dynamic_v3_outcome_due_update_ready_command",
    "dynamic_v3_validate_outcome_due_command",
]
