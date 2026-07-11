from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
    real_snapshot_dry_run_report_payload,
    run_real_snapshot_dry_run,
    validate_real_snapshot_dry_run,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_real_snapshot_dry_run_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_real_snapshot_dry_run_app.command("run")
def dynamic_v3_real_snapshot_dry_run_command(
    snapshot_intake_id: Annotated[
        str,
        typer.Option("--snapshot-intake-id", help="real snapshot intake id。"),
    ],
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot dry-run artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
) -> None:
    """运行 TRADING-205 real snapshot advisory dry run。"""
    result = run_real_snapshot_dry_run(
        snapshot_intake_id=snapshot_intake_id,
        shadow_shortlist_id=shadow_shortlist_id,
        output_dir=output_dir,
    )
    summary = result["real_snapshot_dry_run_summary"]
    typer.echo(f"dry_run_id={result['dry_run_id']}")
    typer.echo(f"snapshot_status={summary['snapshot_status']}")
    typer.echo(f"exposure_status={summary['exposure_status']}")
    typer.echo(f"drift_status={summary['drift_status']}")
    typer.echo(f"guardrail_status={summary['guardrail_status']}")
    typer.echo(f"recommended_action={summary['manual_review_recommended_action']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")


@dynamic_v3_real_snapshot_dry_run_app.command("report")
def dynamic_v3_real_snapshot_dry_run_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest real snapshot dry run。"),
    ] = False,
    dry_run_id: Annotated[str | None, typer.Option("--dry-run-id", help="dry run id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot dry-run artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
) -> None:
    """展示 TRADING-205 real snapshot dry-run 摘要。"""
    payload = real_snapshot_dry_run_report_payload(
        dry_run_id=dry_run_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = mapping_obj(payload.get("real_snapshot_dry_run_summary"))
    typer.echo(f"dry_run_id={payload['dry_run_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"exposure_status={summary.get('exposure_status')}")
    typer.echo(f"drift_status={summary.get('drift_status')}")
    typer.echo(f"guardrail_status={summary.get('guardrail_status')}")
    typer.echo(f"recommended_action={summary.get('manual_review_recommended_action')}")
    typer.echo(f"report_path={payload['real_snapshot_dry_run_report_path']}")


@dynamic_v3_rescue_app.command("validate-real-snapshot-dry-run")
def dynamic_v3_validate_real_snapshot_dry_run_command(
    dry_run_id: Annotated[str, typer.Option("--dry-run-id", help="dry run id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot dry-run artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
) -> None:
    """校验 TRADING-205 real snapshot dry-run artifact。"""
    payload = validate_real_snapshot_dry_run(dry_run_id=dry_run_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
