from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
    apply_real_snapshot_paper_action,
    real_snapshot_paper_action_report_payload,
    validate_real_snapshot_paper_action,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_real_snapshot_paper_action_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_real_snapshot_paper_action_app.command("apply")
def dynamic_v3_real_snapshot_paper_action_apply_command(
    owner_review_id: Annotated[
        str,
        typer.Option("--owner-review-id", "--review-id", help="owner review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot paper action artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
) -> None:
    """从 TRADING-206 owner review 生成 TRADING-207 paper/manual action tracking。"""
    result = apply_real_snapshot_paper_action(
        owner_review_id=owner_review_id,
        output_dir=output_dir,
    )
    action = result["paper_action_from_real_snapshot"]
    typer.echo(f"paper_action_id={result['paper_action_id']}")
    typer.echo(f"action_type={action['action_type']}")
    typer.echo(f"owner_decision={action['owner_decision']}")
    typer.echo("broker_action_taken=false")
    typer.echo("order_ticket_generated=false")


@dynamic_v3_real_snapshot_paper_action_app.command("report")
def dynamic_v3_real_snapshot_paper_action_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest real snapshot paper action。"),
    ] = False,
    paper_action_id: Annotated[
        str | None,
        typer.Option("--paper-action-id", help="paper action id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot paper action artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
) -> None:
    """展示 TRADING-207 paper/manual action 摘要。"""
    payload = real_snapshot_paper_action_report_payload(
        paper_action_id=paper_action_id,
        latest=latest,
        output_dir=output_dir,
    )
    action = mapping_obj(payload.get("paper_action_from_real_snapshot"))
    typer.echo(f"paper_action_id={payload['paper_action_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"action_type={action.get('action_type')}")
    typer.echo(f"broker_action_taken={action.get('broker_action_taken')}")
    typer.echo(f"report_path={payload['real_snapshot_paper_action_report_path']}")


@dynamic_v3_rescue_app.command("validate-real-snapshot-paper-action")
def dynamic_v3_validate_real_snapshot_paper_action_command(
    paper_action_id: Annotated[
        str,
        typer.Option("--paper-action-id", help="paper action id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real snapshot paper action artifact root。"),
    ] = DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
) -> None:
    """校验 TRADING-207 real snapshot paper action artifact。"""
    payload = validate_real_snapshot_paper_action(
        paper_action_id=paper_action_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    typer.echo("order_ticket_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
