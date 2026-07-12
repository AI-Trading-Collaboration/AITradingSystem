from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILL_REPAIR_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    DEFAULT_REPLAY_DIAGNOSIS_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_OUTCOME_DASHBOARD_DIR,
    DEFAULT_OUTCOME_DASHBOARD_POLICY_PATH,
    DEFAULT_OUTCOME_DUE_DIR,
    build_outcome_dashboard,
    outcome_dashboard_report_payload,
    validate_outcome_dashboard_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_outcome_dashboard_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_outcome_dashboard_app.command("build")
def dynamic_v3_outcome_dashboard_build_command(
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome dashboard artifact root。")
    ] = DEFAULT_OUTCOME_DASHBOARD_DIR,
    advisory_outcome_dir: Annotated[
        Path, typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。")
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    backfill_dir: Annotated[
        Path, typer.Option("--backfill-dir", help="backfilled outcome artifact root。")
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Annotated[
        Path, typer.Option("--repair-dir", help="backfill repair artifact root。")
    ] = DEFAULT_BACKFILL_REPAIR_DIR,
    paper_sim_dir: Annotated[
        Path, typer.Option("--paper-sim-dir", help="historical paper simulation root。")
    ] = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    diagnosis_dir: Annotated[
        Path, typer.Option("--diagnosis-dir", help="replay diagnosis artifact root。")
    ] = DEFAULT_REPLAY_DIAGNOSIS_DIR,
    outcome_due_dir: Annotated[
        Path, typer.Option("--outcome-due-dir", help="outcome due artifact root。")
    ] = DEFAULT_OUTCOME_DUE_DIR,
    policy_path: Annotated[
        Path, typer.Option("--policy-path", help="reviewed dashboard policy。")
    ] = DEFAULT_OUTCOME_DASHBOARD_POLICY_PATH,
) -> None:
    """构建 validated outcome availability dashboard。"""
    result = build_outcome_dashboard(
        output_dir=output_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        backfill_dir=backfill_dir,
        repair_dir=repair_dir,
        paper_sim_dir=paper_sim_dir,
        diagnosis_dir=diagnosis_dir,
        outcome_due_dir=outcome_due_dir,
        policy_path=policy_path,
    )
    matrix = result["outcome_availability_matrix"]["summary"]
    typer.echo(f"dashboard_id={result['dashboard_id']}")
    typer.echo(f"dashboard_dir={result['dashboard_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"forward_outcome={matrix['forward_outcome']}")
    typer.echo(f"historical_replay={matrix['historical_replay']}")
    typer.echo(f"backtest_simulation={matrix['backtest_simulation']}")
    typer.echo(f"top_pending_reason={result['reader_brief']['top_pending_reason']}")
    typer.echo("production_effect=none")


@dynamic_v3_outcome_dashboard_app.command("report")
def dynamic_v3_outcome_dashboard_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest outcome dashboard。")
    ] = False,
    dashboard_id: Annotated[
        str | None, typer.Option("--dashboard-id", help="dashboard id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome dashboard artifact root。")
    ] = DEFAULT_OUTCOME_DASHBOARD_DIR,
) -> None:
    """展示 outcome dashboard 摘要。"""
    payload = outcome_dashboard_report_payload(
        dashboard_id=dashboard_id, latest=latest, output_dir=output_dir
    )
    matrix = payload["outcome_availability_matrix"]["summary"]
    pending = payload["pending_reason_dashboard"]
    top = pending["top_pending_reasons"][0] if pending["top_pending_reasons"] else {}
    typer.echo(f"dashboard_id={payload['dashboard_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"forward_outcome={matrix['forward_outcome']}")
    typer.echo(f"historical_replay={matrix['historical_replay']}")
    typer.echo(f"backtest_simulation={matrix['backtest_simulation']}")
    typer.echo(f"top_pending_reason={top.get('reason', 'MISSING')}")
    typer.echo(f"report_path={payload['outcome_dashboard_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-outcome-dashboard")
def dynamic_v3_validate_outcome_dashboard_command(
    dashboard_id: Annotated[str, typer.Option("--dashboard-id", help="dashboard id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome dashboard artifact root。")
    ] = DEFAULT_OUTCOME_DASHBOARD_DIR,
) -> None:
    """校验 TRADING-153 outcome dashboard artifact。"""
    payload = validate_outcome_dashboard_artifact(
        dashboard_id=dashboard_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_outcome_dashboard_build_command",
    "dynamic_v3_outcome_dashboard_report_command",
    "dynamic_v3_validate_outcome_dashboard_command",
]
