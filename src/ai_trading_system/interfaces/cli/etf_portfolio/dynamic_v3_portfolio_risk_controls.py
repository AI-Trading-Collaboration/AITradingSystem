from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_EXECUTION_GUARDRAILS_CONFIG_PATH,
    DEFAULT_EXECUTION_GUARDRAILS_DIR,
    DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    DEFAULT_PORTFOLIO_EXPOSURE_POLICY_CONFIG_PATH,
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_POSITION_DRIFT_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    execution_guardrails_report_payload,
    portfolio_exposure_report_payload,
    position_drift_report_payload,
    run_execution_guardrails_check,
    run_portfolio_exposure_validation,
    run_position_drift_analysis,
    validate_execution_guardrails_artifact,
    validate_portfolio_exposure_artifact,
    validate_position_drift_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_execution_guardrails_app,
    dynamic_v3_portfolio_exposure_app,
    dynamic_v3_position_drift_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_portfolio_exposure_app.command("validate")
def dynamic_v3_portfolio_exposure_validate_command(
    snapshot_id: Annotated[str, typer.Option("--snapshot-id", help="manual snapshot id。")],
    snapshot_dir: Annotated[
        Path,
        typer.Option("--snapshot-dir", help="manual portfolio snapshot artifact root。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    policy_config_path: Annotated[
        Path,
        typer.Option("--policy-config", help="portfolio exposure policy config。"),
    ] = DEFAULT_PORTFOLIO_EXPOSURE_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio exposure artifact root。"),
    ] = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
) -> None:
    """运行 TRADING-200 portfolio exposure validation。"""
    result = run_portfolio_exposure_validation(
        snapshot_id=snapshot_id,
        snapshot_dir=snapshot_dir,
        policy_config_path=policy_config_path,
        output_dir=output_dir,
    )
    summary = result["exposure_summary"]
    typer.echo(f"exposure_id={result['exposure_id']}")
    typer.echo(f"status={summary['status']}")
    typer.echo(f"tech_weight={summary['tech_weight']}")
    typer.echo(f"semiconductor_weight={summary['semiconductor_weight']}")
    typer.echo(f"defensive_weight={summary['defensive_weight']}")
    typer.echo("broker_action_allowed=false")
    if summary["status"] == "FAIL":
        raise typer.Exit(code=1)


@dynamic_v3_portfolio_exposure_app.command("report")
def dynamic_v3_portfolio_exposure_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest exposure artifact。"),
    ] = False,
    exposure_id: Annotated[str | None, typer.Option("--exposure-id", help="exposure id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio exposure artifact root。"),
    ] = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
) -> None:
    """展示 TRADING-200 portfolio exposure 摘要。"""
    payload = portfolio_exposure_report_payload(
        exposure_id=exposure_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = mapping_obj(payload.get("exposure_summary"))
    typer.echo(f"exposure_id={payload['exposure_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"tech_weight={summary.get('tech_weight')}")
    typer.echo(f"semiconductor_weight={summary.get('semiconductor_weight')}")
    typer.echo(f"defensive_weight={summary.get('defensive_weight')}")
    typer.echo(f"report_path={payload['portfolio_exposure_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-portfolio-exposure")
def dynamic_v3_validate_portfolio_exposure_command(
    exposure_id: Annotated[str, typer.Option("--exposure-id", help="exposure id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio exposure artifact root。"),
    ] = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
) -> None:
    """校验 TRADING-200 portfolio exposure artifact。"""
    payload = validate_portfolio_exposure_artifact(exposure_id=exposure_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_position_drift_app.command("run")
def dynamic_v3_position_drift_run_command(
    snapshot_id: Annotated[str, typer.Option("--snapshot-id", help="manual snapshot id。")],
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    snapshot_dir: Annotated[
        Path,
        typer.Option("--snapshot-dir", help="manual portfolio snapshot artifact root。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="position advisory config。"),
    ] = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position drift artifact root。"),
    ] = DEFAULT_POSITION_DRIFT_DIR,
) -> None:
    """运行 TRADING-201 position drift analysis。"""
    result = run_position_drift_analysis(
        snapshot_id=snapshot_id,
        shadow_shortlist_id=shadow_shortlist_id,
        snapshot_dir=snapshot_dir,
        shadow_shortlist_dir=shadow_shortlist_dir,
        config_path=config_path,
        output_dir=output_dir,
    )
    summary = result["consensus_drift_summary"]
    typer.echo(f"drift_id={result['drift_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"total_abs_drift_to_consensus={summary['total_abs_drift_to_consensus']}")
    typer.echo(f"candidate_agreement_status={summary['candidate_agreement_status']}")
    typer.echo(f"drift_status={summary['drift_status']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_position_drift_app.command("report")
def dynamic_v3_position_drift_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest position drift artifact。"),
    ] = False,
    drift_id: Annotated[str | None, typer.Option("--drift-id", help="drift id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position drift artifact root。"),
    ] = DEFAULT_POSITION_DRIFT_DIR,
) -> None:
    """展示 TRADING-201 position drift 摘要。"""
    payload = position_drift_report_payload(drift_id=drift_id, latest=latest, output_dir=output_dir)
    summary = mapping_obj(payload.get("consensus_drift_summary"))
    actions = mapping_obj(payload.get("drift_action_candidates"))
    typer.echo(f"drift_id={payload['drift_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_abs_drift_to_consensus={summary.get('total_abs_drift_to_consensus')}")
    typer.echo(f"candidate_agreement_status={summary.get('candidate_agreement_status')}")
    typer.echo(f"recommended_action={actions.get('recommended_action')}")
    typer.echo(f"report_path={payload['position_drift_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-position-drift")
def dynamic_v3_validate_position_drift_command(
    drift_id: Annotated[str, typer.Option("--drift-id", help="position drift id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="position drift artifact root。"),
    ] = DEFAULT_POSITION_DRIFT_DIR,
) -> None:
    """校验 TRADING-201 position drift artifact。"""
    payload = validate_position_drift_artifact(drift_id=drift_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_execution_guardrails_app.command("check")
def dynamic_v3_execution_guardrails_check_command(
    drift_id: Annotated[str, typer.Option("--drift-id", help="position drift id。")],
    exposure_id: Annotated[str, typer.Option("--exposure-id", help="portfolio exposure id。")],
    drift_dir: Annotated[
        Path,
        typer.Option("--drift-dir", help="position drift artifact root。"),
    ] = DEFAULT_POSITION_DRIFT_DIR,
    exposure_dir: Annotated[
        Path,
        typer.Option("--exposure-dir", help="portfolio exposure artifact root。"),
    ] = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="execution guardrails config。"),
    ] = DEFAULT_EXECUTION_GUARDRAILS_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="execution guardrails artifact root。"),
    ] = DEFAULT_EXECUTION_GUARDRAILS_DIR,
) -> None:
    """运行 TRADING-202 execution guardrail checks。"""
    result = run_execution_guardrails_check(
        drift_id=drift_id,
        exposure_id=exposure_id,
        drift_dir=drift_dir,
        exposure_dir=exposure_dir,
        config_path=config_path,
        output_dir=output_dir,
    )
    summary = result["guardrail_summary"]
    typer.echo(f"guardrail_id={result['guardrail_id']}")
    typer.echo(f"recommended_action={summary['recommended_action']}")
    typer.echo(f"capped_count={summary['capped_count']}")
    typer.echo(f"blocked_count={summary['blocked_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generation_allowed=false")


@dynamic_v3_execution_guardrails_app.command("report")
def dynamic_v3_execution_guardrails_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest guardrail artifact。"),
    ] = False,
    guardrail_id: Annotated[
        str | None,
        typer.Option("--guardrail-id", help="guardrail id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="execution guardrails artifact root。"),
    ] = DEFAULT_EXECUTION_GUARDRAILS_DIR,
) -> None:
    """展示 TRADING-202 execution guardrails 摘要。"""
    payload = execution_guardrails_report_payload(
        guardrail_id=guardrail_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = mapping_obj(payload.get("guardrail_summary"))
    typer.echo(f"guardrail_id={payload['guardrail_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_action={summary.get('recommended_action')}")
    typer.echo(f"capped_count={summary.get('capped_count')}")
    typer.echo(f"blocked_count={summary.get('blocked_count')}")
    typer.echo(f"report_path={payload['execution_guardrails_report_path']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generation_allowed=false")


@dynamic_v3_rescue_app.command("validate-execution-guardrails")
def dynamic_v3_validate_execution_guardrails_command(
    guardrail_id: Annotated[str, typer.Option("--guardrail-id", help="guardrail id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="execution guardrails artifact root。"),
    ] = DEFAULT_EXECUTION_GUARDRAILS_DIR,
) -> None:
    """校验 TRADING-202 execution guardrails artifact。"""
    payload = validate_execution_guardrails_artifact(
        guardrail_id=guardrail_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generation_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
