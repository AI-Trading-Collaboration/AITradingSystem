from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    DEFAULT_REAL_SNAPSHOT_INTAKE_DIR,
    DEFAULT_REAL_SNAPSHOT_TEMPLATE_PATH,
    intake_real_snapshot,
    lint_real_snapshot_file,
    real_snapshot_report_payload,
    validate_real_snapshot,
    write_real_snapshot_template,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_real_snapshot_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_real_snapshot_app.command("template")
def dynamic_v3_real_snapshot_template_command(
    output_path: Annotated[
        Path,
        typer.Option("--output", "--output-path", help="redaction-safe real snapshot template。"),
    ] = DEFAULT_REAL_SNAPSHOT_TEMPLATE_PATH,
) -> None:
    """生成 TRADING-204 redaction-safe real manual snapshot template。"""
    payload = write_real_snapshot_template(output_path)
    typer.echo(f"template_path={payload['template_path']}")
    typer.echo(f"status={payload['status']}")
    typer.echo("broker_imported=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")


@dynamic_v3_real_snapshot_app.command("lint")
def dynamic_v3_real_snapshot_lint_command(
    snapshot: Annotated[Path, typer.Option("--snapshot", help="real manual snapshot YAML。")],
) -> None:
    """检查 TRADING-204 real manual snapshot 是否 redaction-safe。"""
    payload = lint_real_snapshot_file(snapshot)
    typer.echo(f"redaction_status={payload['redaction_status']}")
    typer.echo(f"blocking_issues={len(payload['blocking_issues'])}")
    typer.echo(f"warnings={len(payload['warnings'])}")
    typer.echo("broker_imported=false")
    typer.echo("broker_action_taken=false")
    if payload["redaction_status"] == "FAIL":
        raise typer.Exit(code=1)


@dynamic_v3_real_snapshot_app.command("intake")
def dynamic_v3_real_snapshot_intake_command(
    snapshot: Annotated[Path, typer.Option("--snapshot", help="real manual snapshot YAML。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot intake artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_INTAKE_DIR,
) -> None:
    """接入 TRADING-204 owner-maintained real manual snapshot。"""
    result = intake_real_snapshot(snapshot_path=snapshot, output_dir=output_dir)
    manifest = result["manifest"]
    typer.echo(f"snapshot_intake_id={result['snapshot_intake_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"redaction_status={manifest['redaction_status']}")
    typer.echo(f"snapshot_status={manifest['snapshot_status']}")
    typer.echo(f"manual_portfolio_snapshot_id={manifest['manual_portfolio_snapshot_id']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("broker_action_taken=false")
    if manifest["status"] == "FAIL":
        raise typer.Exit(code=1)


@dynamic_v3_real_snapshot_app.command("report")
def dynamic_v3_real_snapshot_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest real snapshot intake。"),
    ] = False,
    snapshot_intake_id: Annotated[
        str | None,
        typer.Option("--snapshot-intake-id", help="real snapshot intake id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot intake artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_INTAKE_DIR,
) -> None:
    """展示 TRADING-204 real snapshot intake 摘要。"""
    payload = real_snapshot_report_payload(
        snapshot_intake_id=snapshot_intake_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"snapshot_intake_id={payload['snapshot_intake_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"redaction_status={payload['redaction_status']}")
    typer.echo(f"snapshot_status={payload['snapshot_status']}")
    typer.echo(f"report_path={payload['real_snapshot_intake_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-real-snapshot")
def dynamic_v3_validate_real_snapshot_command(
    snapshot_intake_id: Annotated[
        str,
        typer.Option("--snapshot-intake-id", help="real snapshot intake id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot intake artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_INTAKE_DIR,
) -> None:
    """校验 TRADING-204 real snapshot intake artifact。"""
    payload = validate_real_snapshot(snapshot_intake_id=snapshot_intake_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
