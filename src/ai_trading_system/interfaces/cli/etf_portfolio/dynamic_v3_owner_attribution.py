from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_OWNER_ATTRIBUTION_DIR,
    owner_attribution_report_payload,
    run_owner_attribution,
    validate_owner_attribution_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_owner_attribution_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_owner_attribution_app.command("run")
def dynamic_v3_owner_attribution_run_command(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner attribution artifact root。"),
    ] = DEFAULT_OWNER_ATTRIBUTION_DIR,
    owner_review_dir: Annotated[
        Path,
        typer.Option("--owner-review-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    outcome_dir: Annotated[
        Path,
        typer.Option("--outcome-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> None:
    """生成 TRADING-138 owner attribution report。"""
    result = run_owner_attribution(
        output_dir=output_dir,
        owner_review_dir=owner_review_dir,
        outcome_dir=outcome_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"attribution_id={result['attribution_id']}")
    typer.echo(f"attribution_dir={result['attribution_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"total_reviews={manifest['total_reviews']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_owner_attribution_app.command("report")
def dynamic_v3_owner_attribution_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner attribution。"),
    ] = False,
    attribution_id: Annotated[
        str | None,
        typer.Option("--attribution-id", help="attribution id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner attribution artifact root。"),
    ] = DEFAULT_OWNER_ATTRIBUTION_DIR,
) -> None:
    """展示 TRADING-138 owner attribution 摘要。"""
    payload = owner_attribution_report_payload(
        attribution_id=attribution_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"attribution_id={payload['attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"total_reviews={payload['total_reviews']}")
    typer.echo(f"report_path={payload['owner_attribution_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-owner-attribution")
def dynamic_v3_validate_owner_attribution_command(
    attribution_id: Annotated[
        str,
        typer.Option("--attribution-id", help="attribution id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner attribution artifact root。"),
    ] = DEFAULT_OWNER_ATTRIBUTION_DIR,
) -> None:
    """校验 TRADING-138 owner attribution artifact。"""
    payload = validate_owner_attribution_artifact(
        attribution_id=attribution_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_owner_attribution_report_command",
    "dynamic_v3_owner_attribution_run_command",
    "dynamic_v3_validate_owner_attribution_command",
]
