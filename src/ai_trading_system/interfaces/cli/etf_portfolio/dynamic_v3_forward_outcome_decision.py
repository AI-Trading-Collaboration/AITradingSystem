from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_EVIDENCE_TREND_DIR,
    DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
    DEFAULT_FORWARD_OUTCOME_DECISION_POLICY_PATH,
    DEFAULT_OUTCOME_UPDATE_DIR,
    DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    forward_outcome_decision_report_payload,
    run_forward_outcome_decision,
    validate_forward_outcome_decision_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_forward_outcome_decision_app,
    dynamic_v3_rescue_app,
)


def _parse_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD") from exc


@dynamic_v3_forward_outcome_decision_app.command("run")
def dynamic_v3_forward_outcome_decision_run_command(
    week_ending: Annotated[str, typer.Option("--week-ending", help="周度结束日期 YYYY-MM-DD。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward outcome decision artifact root。"),
    ] = DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
    outcome_update_dir: Annotated[
        Path,
        typer.Option("--outcome-update-dir", help="Outcome Update artifact root。"),
    ] = DEFAULT_OUTCOME_UPDATE_DIR,
    rolling_refresh_dir: Annotated[
        Path,
        typer.Option("--rolling-refresh-dir", help="Rolling Refresh artifact root。"),
    ] = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    evidence_trend_dir: Annotated[
        Path,
        typer.Option("--evidence-trend-dir", help="Evidence Trend artifact root。"),
    ] = DEFAULT_EVIDENCE_TREND_DIR,
    outcome_update_id: Annotated[
        str | None,
        typer.Option("--outcome-update-id", help="显式Outcome Update id。"),
    ] = None,
    refresh_id: Annotated[
        str | None,
        typer.Option("--refresh-id", help="显式Rolling Refresh id。"),
    ] = None,
    trend_id: Annotated[
        str | None,
        typer.Option("--trend-id", help="显式Evidence Trend id。"),
    ] = None,
    policy_path: Annotated[
        Path,
        typer.Option("--policy-path", help="reviewed forward decision policy path。"),
    ] = DEFAULT_FORWARD_OUTCOME_DECISION_POLICY_PATH,
) -> None:
    """生成 weekly forward outcome decision pack。"""
    result = run_forward_outcome_decision(
        week_ending=_parse_date(week_ending, "--week-ending"),
        output_dir=output_dir,
        outcome_update_dir=outcome_update_dir,
        rolling_refresh_dir=rolling_refresh_dir,
        evidence_trend_dir=evidence_trend_dir,
        outcome_update_id=outcome_update_id,
        refresh_id=refresh_id,
        trend_id=trend_id,
        policy_path=policy_path,
    )
    matrix = result["forward_go_no_go_matrix"]
    actions = result["forward_next_actions"]["next_actions"]
    next_due = next(
        (row.get("target_date") for row in actions if row.get("action") == "run_next_due_scan"),
        "",
    )
    typer.echo(f"decision_id={result['decision_id']}")
    typer.echo(f"decision_dir={result['decision_dir']}")
    typer.echo(f"evidence_status={matrix['evidence_status']}")
    typer.echo(f"recommended_action={matrix['recommended_action']}")
    typer.echo(f"rule_calibration_readiness={matrix['rule_calibration_readiness']}")
    typer.echo(f"next_due_scan_date={next_due}")
    typer.echo(f"policy_id={result['manifest']['policy_id']}")
    typer.echo(f"policy_version={result['manifest']['policy_version']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_forward_outcome_decision_app.command("report")
def dynamic_v3_forward_outcome_decision_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest forward outcome decision。"),
    ] = False,
    decision_id: Annotated[
        str | None,
        typer.Option("--decision-id", help="decision id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward outcome decision artifact root。"),
    ] = DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
) -> None:
    """展示 weekly forward outcome decision 摘要。"""
    payload = forward_outcome_decision_report_payload(
        decision_id=decision_id,
        latest=latest,
        output_dir=output_dir,
    )
    matrix = payload["forward_go_no_go_matrix"]
    actions = payload["forward_next_actions"]["next_actions"]
    next_due = next(
        (row.get("target_date") for row in actions if row.get("action") == "run_next_due_scan"),
        "",
    )
    typer.echo(f"decision_id={payload['decision_id']}")
    typer.echo(f"evidence_status={matrix['evidence_status']}")
    typer.echo(f"recommended_action={matrix['recommended_action']}")
    typer.echo(f"rule_calibration_readiness={matrix['rule_calibration_readiness']}")
    typer.echo(f"next_due_scan_date={next_due}")
    typer.echo(f"policy_id={payload['policy_id']}")
    typer.echo(f"policy_version={payload['policy_version']}")
    typer.echo(f"report_path={payload['forward_outcome_decision_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-forward-outcome-decision")
def dynamic_v3_validate_forward_outcome_decision_command(
    decision_id: Annotated[str, typer.Option("--decision-id", help="decision id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward outcome decision artifact root。"),
    ] = DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
) -> None:
    """校验 TRADING-160 forward outcome decision artifact。"""
    payload = validate_forward_outcome_decision_artifact(
        decision_id=decision_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_forward_outcome_decision_run_command",
    "dynamic_v3_forward_outcome_decision_report_command",
    "dynamic_v3_validate_forward_outcome_decision_command",
]
