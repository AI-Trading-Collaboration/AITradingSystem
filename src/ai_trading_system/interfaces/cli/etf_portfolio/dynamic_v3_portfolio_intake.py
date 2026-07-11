from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
    manual_portfolio_report_payload,
    portfolio_snapshot_report_payload,
    validate_manual_portfolio_artifact,
    validate_manual_portfolio_snapshot_file,
    validate_portfolio_snapshot_file,
    write_portfolio_snapshot_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_manual_portfolio_app,
    dynamic_v3_portfolio_snapshot_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_portfolio_snapshot_app.command("validate")
def dynamic_v3_portfolio_snapshot_validate_command(
    snapshot: Annotated[Path, typer.Option("--snapshot", help="portfolio snapshot YAML。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio snapshot artifact root。"),
    ] = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """校验 TRADING-132 manual portfolio snapshot。"""
    payload = validate_portfolio_snapshot_file(snapshot_path=snapshot, output_dir=output_dir)
    typer.echo(f"snapshot_id={payload['snapshot_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"snapshot_dir={payload['snapshot_dir']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_portfolio_snapshot_app.command("report")
def dynamic_v3_portfolio_snapshot_report_command(
    snapshot: Annotated[
        Path | None,
        typer.Option("--snapshot", help="portfolio snapshot YAML。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest portfolio snapshot artifact。"),
    ] = False,
    snapshot_id: Annotated[
        str | None,
        typer.Option("--snapshot-id", help="snapshot artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio snapshot artifact root。"),
    ] = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """生成或展示 TRADING-132 portfolio snapshot report。"""
    payload = portfolio_snapshot_report_payload(
        snapshot_path=snapshot,
        snapshot_id=snapshot_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"snapshot_id={payload['snapshot_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"manual_review_required={payload['manual_review_required']}")
    typer.echo(f"report_path={payload['snapshot_validation_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_portfolio_snapshot_app.command("normalize")
def dynamic_v3_portfolio_snapshot_normalize_command(
    snapshot: Annotated[Path, typer.Option("--snapshot", help="portfolio snapshot YAML。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="portfolio snapshot artifact root。"),
    ] = DEFAULT_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """输出 TRADING-132 normalized portfolio snapshot artifact。"""
    result = write_portfolio_snapshot_artifact(snapshot_path=snapshot, output_dir=output_dir)
    manifest = result["manifest"]
    typer.echo(f"snapshot_id={result['snapshot_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"normalized_positions_path={manifest['normalized_positions_path']}")
    typer.echo("broker_action_allowed=false")
    if manifest["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_manual_portfolio_app.command("validate")
def dynamic_v3_manual_portfolio_validate_command(
    snapshot: Annotated[Path, typer.Option("--snapshot", help="manual portfolio snapshot YAML。")],
    schema_config_path: Annotated[
        Path,
        typer.Option("--schema-config", help="manual portfolio schema config。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="manual portfolio snapshot artifact root。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """校验 TRADING-199 hardened manual portfolio snapshot。"""
    payload = validate_manual_portfolio_snapshot_file(
        snapshot_path=snapshot,
        schema_config_path=schema_config_path,
        output_dir=output_dir,
    )
    typer.echo(f"snapshot_id={payload['snapshot_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"snapshot_dir={payload['snapshot_dir']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_manual_portfolio_app.command("normalize")
def dynamic_v3_manual_portfolio_normalize_command(
    snapshot: Annotated[Path, typer.Option("--snapshot", help="manual portfolio snapshot YAML。")],
    schema_config_path: Annotated[
        Path,
        typer.Option("--schema-config", help="manual portfolio schema config。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="manual portfolio snapshot artifact root。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """生成 TRADING-199 normalized_portfolio artifact。"""
    result = manual_portfolio_report_payload(
        snapshot_path=snapshot,
        schema_config_path=schema_config_path,
        output_dir=output_dir,
    )
    normalized = mapping_obj(result.get("normalized_portfolio"))
    typer.echo(f"snapshot_id={result['snapshot_id']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"total_weight={normalized.get('total_weight')}")
    typer.echo(f"cash_weight={normalized.get('cash_weight')}")
    typer.echo(f"risk_asset_weight={normalized.get('risk_asset_weight')}")
    typer.echo(f"normalized_portfolio_path={result['normalized_portfolio_path']}")
    typer.echo("broker_action_allowed=false")
    if result["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_manual_portfolio_app.command("report")
def dynamic_v3_manual_portfolio_report_command(
    snapshot: Annotated[
        Path | None,
        typer.Option("--snapshot", help="manual portfolio snapshot YAML。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest manual portfolio artifact。"),
    ] = False,
    snapshot_id: Annotated[
        str | None,
        typer.Option("--snapshot-id", help="manual portfolio snapshot id。"),
    ] = None,
    schema_config_path: Annotated[
        Path,
        typer.Option("--schema-config", help="manual portfolio schema config。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="manual portfolio snapshot artifact root。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """展示 TRADING-199 manual portfolio snapshot 摘要。"""
    payload = manual_portfolio_report_payload(
        snapshot_path=snapshot,
        snapshot_id=snapshot_id,
        latest=latest,
        schema_config_path=schema_config_path,
        output_dir=output_dir,
    )
    normalized = mapping_obj(payload.get("normalized_portfolio"))
    typer.echo(f"snapshot_id={payload['snapshot_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_weight={normalized.get('total_weight')}")
    typer.echo(f"cash_weight={normalized.get('cash_weight')}")
    typer.echo(f"risk_asset_weight={normalized.get('risk_asset_weight')}")
    typer.echo(f"report_path={payload['portfolio_snapshot_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-manual-portfolio")
def dynamic_v3_validate_manual_portfolio_command(
    snapshot_id: Annotated[str, typer.Option("--snapshot-id", help="manual snapshot id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="manual portfolio snapshot artifact root。"),
    ] = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
) -> None:
    """校验 TRADING-199 manual portfolio artifact。"""
    payload = validate_manual_portfolio_artifact(snapshot_id=snapshot_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
