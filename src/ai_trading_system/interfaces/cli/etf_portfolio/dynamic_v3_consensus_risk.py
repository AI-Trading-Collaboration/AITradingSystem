from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILL_REPAIR_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_HISTORICAL_REPLAY_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_CONSENSUS_RISK_DIR,
    DEFAULT_CONSENSUS_RISK_POLICY_PATH,
    consensus_risk_report_payload,
    run_consensus_risk_review,
    validate_consensus_risk_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_consensus_risk_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_consensus_risk_app.command("run")
def dynamic_v3_consensus_risk_run_command(
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="consensus risk artifact root。")
    ] = DEFAULT_CONSENSUS_RISK_DIR,
    daily_advisory_dir: Annotated[
        Path, typer.Option("--daily-advisory-dir", help="daily advisory artifact root。")
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    historical_replay_dir: Annotated[
        Path, typer.Option("--historical-replay-dir", help="historical replay artifact root。")
    ] = DEFAULT_HISTORICAL_REPLAY_DIR,
    backfill_dir: Annotated[
        Path, typer.Option("--backfill-dir", help="backfilled outcome artifact root。")
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Annotated[
        Path, typer.Option("--repair-dir", help="backfill repair artifact root。")
    ] = DEFAULT_BACKFILL_REPAIR_DIR,
    policy_path: Annotated[
        Path, typer.Option("--policy-path", help="reviewed consensus risk policy。")
    ] = DEFAULT_CONSENSUS_RISK_POLICY_PATH,
) -> None:
    """运行 validated consensus_target 风险审查。"""
    result = run_consensus_risk_review(
        output_dir=output_dir,
        daily_advisory_dir=daily_advisory_dir,
        historical_replay_dir=historical_replay_dir,
        backfill_dir=backfill_dir,
        repair_dir=repair_dir,
        policy_path=policy_path,
    )
    exposure = result["consensus_exposure_summary"]
    drawdown = result["consensus_drawdown_risk"]["window_results"][0]
    turnover = result["consensus_turnover_risk"]
    typer.echo(f"risk_id={result['risk_id']}")
    typer.echo(f"risk_dir={result['risk_dir']}")
    typer.echo(f"consensus_target_risk={result['manifest']['consensus_target_risk']}")
    typer.echo(f"risk_asset_exposure={exposure['risk_asset_exposure']}")
    typer.echo(f"drawdown_risk={drawdown['risk_status']}")
    typer.echo(f"turnover_risk={turnover['turnover_status']}")
    typer.echo("consensus_target_default_execution_recommended=false")
    typer.echo("production_effect=none")


@dynamic_v3_consensus_risk_app.command("report")
def dynamic_v3_consensus_risk_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest consensus risk artifact。")
    ] = False,
    risk_id: Annotated[str | None, typer.Option("--risk-id", help="risk id。")]=None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="consensus risk artifact root。")
    ] = DEFAULT_CONSENSUS_RISK_DIR,
) -> None:
    """展示 consensus_target risk 摘要。"""
    payload = consensus_risk_report_payload(
        risk_id=risk_id, latest=latest, output_dir=output_dir
    )
    exposure = payload["consensus_exposure_summary"]
    drawdown = payload["consensus_drawdown_risk"]["window_results"][0]
    turnover = payload["consensus_turnover_risk"]
    typer.echo(f"risk_id={payload['risk_id']}")
    typer.echo(f"consensus_target_risk={payload['consensus_target_risk']}")
    typer.echo(f"risk_asset_exposure={exposure['risk_asset_exposure']}")
    typer.echo(f"drawdown_risk={drawdown['risk_status']}")
    typer.echo(f"turnover_risk={turnover['turnover_status']}")
    typer.echo(f"report_path={payload['consensus_risk_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-consensus-risk")
def dynamic_v3_validate_consensus_risk_command(
    risk_id: Annotated[str, typer.Option("--risk-id", help="risk id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="consensus risk artifact root。")
    ] = DEFAULT_CONSENSUS_RISK_DIR,
) -> None:
    """校验 TRADING-155 consensus risk artifact。"""
    payload = validate_consensus_risk_artifact(risk_id=risk_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("consensus_target_default_execution_recommended=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_consensus_risk_run_command",
    "dynamic_v3_consensus_risk_report_command",
    "dynamic_v3_validate_consensus_risk_command",
]
