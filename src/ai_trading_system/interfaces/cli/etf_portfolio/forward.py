from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.experiments import DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH
from ai_trading_system.etf_portfolio.forward import (
    DEFAULT_ETF_FORWARD_CONFIG_PATH,
    DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    DEFAULT_ETF_FORWARD_REPORT_DIR,
    run_forward_dashboard,
    run_forward_update,
    run_forward_validation,
    run_forward_watchlist,
    run_forward_weekly_review,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import forward_app


@forward_app.command("update")
def forward_update_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="forward evaluation 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 ETF price cache 最新日期。"),
    ] = False,
    config_path: Annotated[
        Path,
        typer.Option(help="TRADING-065 forward simulation policy config。"),
    ] = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Annotated[
        Path,
        typer.Option(help="decision-time ledger 输出路径。"),
    ] = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    output_dir: Annotated[Path, typer.Option(help="forward update 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "updates"
    ),
) -> None:
    """更新 active shadow candidates 的 evaluation-only forward performance。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = _resolve_date(
        "latest" if latest or date_option is None else date_option,
        prices_path=prices_path,
    )
    try:
        payload = run_forward_update(
            as_of=run_date,
            config_path=config_path,
            registry_path=registry_path,
            decision_ledger_path=decision_ledger_path,
            prices_path=prices_path,
            output_dir=output_dir,
        )
    except ValueError as exc:
        typer.echo(f"ETF forward update blocked: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF forward update：{output_dir / f'forward_update_{run_date.isoformat()}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['active_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@forward_app.command("dashboard")
def forward_dashboard_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="dashboard 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest forward update artifact。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    update_dir: Annotated[Path, typer.Option(help="forward update artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "updates"
    ),
    output_dir: Annotated[Path, typer.Option(help="dashboard 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
) -> None:
    """生成 candidate vs baseline vs benchmark forward dashboard。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_dashboard(
        as_of=run_date,
        latest=latest or date_option is None,
        registry_path=registry_path,
        update_dir=update_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward dashboard：{output_dir / f'forward_dashboard_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['status_summary']['active_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@forward_app.command("weekly-review")
def forward_weekly_review_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="weekly review 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest dashboard artifact。"),
    ] = False,
    dashboard_dir: Annotated[Path, typer.Option(help="forward dashboard artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "weekly_reviews"
    ),
) -> None:
    """生成 TRADING-065 observe-only weekly review。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_weekly_review(
        as_of=run_date,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward weekly review：{output_dir / f'weekly_review_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['review_period']['candidate_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@forward_app.command("watchlist")
def forward_watchlist_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="watchlist 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 latest dashboard artifact。"),
    ] = False,
    dashboard_dir: Annotated[Path, typer.Option(help="forward dashboard artifacts 目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard"
    ),
    output_dir: Annotated[Path, typer.Option(help="watchlist 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "watchlist"
    ),
) -> None:
    """生成本地 ETF forward watchlist；不发送外部 alert。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = None if latest or date_option is None else _parse_date(date_option)
    payload = run_forward_watchlist(
        as_of=run_date,
        dashboard_dir=dashboard_dir,
        output_dir=output_dir,
    )
    artifact_date = payload["as_of"]
    typer.echo(f"ETF forward watchlist：{output_dir / f'forward_watchlist_{artifact_date}.md'}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"attention_count={payload['summary']['item_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@forward_app.command("validate")
def forward_validate_command(
    config_path: Annotated[
        Path,
        typer.Option(help="TRADING-065 forward simulation policy config。"),
    ] = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Annotated[
        Path,
        typer.Option(help="decision-time ledger 路径。"),
    ] = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = PROJECT_ROOT / "config" / "report_registry.yaml",
    output_dir: Annotated[Path, typer.Option(help="validation 输出目录。")] = (
        DEFAULT_ETF_FORWARD_REPORT_DIR / "validation"
    ),
) -> None:
    """生成 TRADING-065 final forward simulation validation gate。"""
    payload = run_forward_validation(
        config_path=config_path,
        registry_path=registry_path,
        decision_ledger_path=decision_ledger_path,
        report_registry_path=report_registry_path,
        output_dir=output_dir,
    )
    typer.echo(f"ETF forward validation gate：{output_dir}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
