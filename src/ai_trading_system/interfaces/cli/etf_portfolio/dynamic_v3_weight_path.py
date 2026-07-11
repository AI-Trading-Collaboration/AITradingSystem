from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    validate_weight_path_artifact,
    weight_path_report_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_weight_path_app,
)


@dynamic_v3_weight_path_app.command("validate")
def dynamic_v3_weight_path_validate_command(
    evaluation_id: Annotated[str, typer.Option("--evaluation-id", help="real evaluation id。")],
    search_root: Annotated[
        Path,
        typer.Option("--search-root", help="weight path 搜索根目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> None:
    """校验 TRADING-112 weight path artifacts。"""
    payload = validate_weight_path_artifact(evaluation_id=evaluation_id, search_root=search_root)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"attribution_completeness={payload['attribution_completeness']}")
    typer.echo(
        "declared_attribution_completeness="
        f"{payload['declared_attribution_completeness']}"
    )
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"limitations={','.join(payload['limitations'])}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_path_app.command("report")
def dynamic_v3_weight_path_report_command(
    evaluation_id: Annotated[str, typer.Option("--evaluation-id", help="real evaluation id。")],
    search_root: Annotated[
        Path,
        typer.Option("--search-root", help="weight path 搜索根目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
) -> None:
    """展示 TRADING-112 weight path 摘要。"""
    payload = weight_path_report_payload(evaluation_id=evaluation_id, search_root=search_root)
    typer.echo(f"evaluation_id={evaluation_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        "declared_attribution_completeness="
        f"{payload['declared_attribution_completeness']}"
    )
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"limitations={','.join(payload['limitations'])}")
    typer.echo(f"daily_weights_path={payload['daily_weights_path']}")
    typer.echo(f"weight_path_metadata_path={payload['weight_path_metadata_path']}")
    typer.echo("production_candidate_generated=false")
