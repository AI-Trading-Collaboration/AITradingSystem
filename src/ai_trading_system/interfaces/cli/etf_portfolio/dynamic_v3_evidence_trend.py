from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_EVIDENCE_TREND_DIR,
    DEFAULT_EVIDENCE_TREND_POLICY_PATH,
    DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    evidence_trend_report_payload,
    run_evidence_trend,
    validate_evidence_trend_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_evidence_trend_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_evidence_trend_app.command("run")
def dynamic_v3_evidence_trend_run_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence trend artifact root。"),
    ] = DEFAULT_EVIDENCE_TREND_DIR,
    rolling_refresh_dir: Annotated[
        Path,
        typer.Option("--rolling-refresh-dir", help="rolling evidence refresh artifact root。"),
    ] = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    policy_path: Annotated[
        Path,
        typer.Option("--policy-path", help="reviewed evidence trend policy path。"),
    ] = DEFAULT_EVIDENCE_TREND_POLICY_PATH,
) -> None:
    """构建 rolling refresh evidence trend。"""
    result = run_evidence_trend(
        output_dir=output_dir,
        rolling_refresh_dir=rolling_refresh_dir,
        policy_path=policy_path,
    )
    summary = result["confidence_trend_summary"]
    typer.echo(f"trend_id={result['trend_id']}")
    typer.echo(f"trend_dir={result['trend_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"trend_status={summary['trend_status']}")
    typer.echo(f"confidence_change={summary['confidence_change']}")
    typer.echo(f"next_action={summary['next_action']}")
    typer.echo(f"policy_id={summary['policy_id']}")
    typer.echo(f"policy_version={summary['policy_version']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_evidence_trend_app.command("report")
def dynamic_v3_evidence_trend_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest evidence trend。"),
    ] = False,
    trend_id: Annotated[str | None, typer.Option("--trend-id", help="trend id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence trend artifact root。"),
    ] = DEFAULT_EVIDENCE_TREND_DIR,
) -> None:
    """展示 evidence trend 摘要。"""
    payload = evidence_trend_report_payload(trend_id=trend_id, latest=latest, output_dir=output_dir)
    summary = payload["confidence_trend_summary"]
    typer.echo(f"trend_id={payload['trend_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"trend_status={summary['trend_status']}")
    typer.echo(f"confidence_change={summary['confidence_change']}")
    typer.echo(f"next_action={summary['next_action']}")
    typer.echo(f"policy_id={summary['policy_id']}")
    typer.echo(f"policy_version={summary['policy_version']}")
    typer.echo(f"report_path={payload['evidence_trend_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-evidence-trend")
def dynamic_v3_validate_evidence_trend_command(
    trend_id: Annotated[str, typer.Option("--trend-id", help="trend id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence trend artifact root。"),
    ] = DEFAULT_EVIDENCE_TREND_DIR,
) -> None:
    """校验 TRADING-159 evidence trend artifact。"""
    payload = validate_evidence_trend_artifact(trend_id=trend_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_evidence_trend_run_command",
    "dynamic_v3_evidence_trend_report_command",
    "dynamic_v3_validate_evidence_trend_command",
]
