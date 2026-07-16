from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_research_direction_foundation as direction

from .registration import (
    dynamic_v3_next_research_direction_app,
    dynamic_v3_owner_research_roadmap_app,
    dynamic_v3_rescue_app,
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


@dynamic_v3_next_research_direction_app.command("run")
def dynamic_v3_next_research_direction_run_command(
    attribution_id: Annotated[
        str,
        typer.Option("--attribution-id", help="signal-vs-parameter attribution id。"),
    ],
    attribution_dir: Annotated[
        Path,
        typer.Option("--attribution-dir", help="signal-vs-parameter attribution root。"),
    ] = direction.DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="next research direction root。"),
    ] = direction.DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
) -> None:
    result = direction.run_next_research_direction(
        attribution_id=attribution_id,
        attribution_dir=attribution_dir,
        output_dir=output_dir,
    )
    decision = _mapping(result.get("next_research_direction_decision"))
    typer.echo(f"direction_id={result['direction_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_next_research_direction_app.command("report")
def dynamic_v3_next_research_direction_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    direction_id: Annotated[
        str | None,
        typer.Option("--direction-id", help="next research direction id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="next research direction root。"),
    ] = direction.DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
) -> None:
    payload = direction.next_research_direction_report_payload(
        direction_id=direction_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping(payload.get("next_research_direction_decision"))
    typer.echo(f"direction_id={payload['direction_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"report_path={payload['next_research_direction_report_path']}")


@dynamic_v3_rescue_app.command("validate-next-research-direction")
def dynamic_v3_validate_next_research_direction_command(
    direction_id: Annotated[
        str,
        typer.Option("--direction-id", help="next research direction id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="next research direction root。"),
    ] = direction.DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
) -> None:
    payload = direction.validate_next_research_direction_artifact(
        direction_id=direction_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_owner_research_roadmap_app.command("update")
def dynamic_v3_owner_research_roadmap_update_command(
    direction_id: Annotated[
        str,
        typer.Option("--direction-id", help="next research direction id。"),
    ],
    direction_dir: Annotated[
        Path,
        typer.Option("--direction-dir", help="next research direction root。"),
    ] = direction.DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner research roadmap root。"),
    ] = direction.DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
) -> None:
    result = direction.update_owner_research_roadmap(
        direction_id=direction_id,
        direction_dir=direction_dir,
        output_dir=output_dir,
    )
    summary = _mapping(result.get("owner_roadmap_summary"))
    typer.echo(f"roadmap_id={result['roadmap_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"next_research_direction={summary.get('next_research_direction')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_owner_research_roadmap_app.command("report")
def dynamic_v3_owner_research_roadmap_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    roadmap_id: Annotated[
        str | None,
        typer.Option("--roadmap-id", help="owner roadmap id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner research roadmap root。"),
    ] = direction.DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
) -> None:
    payload = direction.owner_research_roadmap_report_payload(
        roadmap_id=roadmap_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping(payload.get("owner_roadmap_summary"))
    typer.echo(f"roadmap_id={payload['roadmap_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"next_research_direction={summary.get('next_research_direction')}")
    typer.echo(f"report_path={payload['owner_research_roadmap_report_path']}")


@dynamic_v3_rescue_app.command("validate-owner-research-roadmap")
def dynamic_v3_validate_owner_research_roadmap_command(
    roadmap_id: Annotated[str, typer.Option("--roadmap-id", help="owner roadmap id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner research roadmap root。"),
    ] = direction.DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
) -> None:
    payload = direction.validate_owner_research_roadmap_artifact(
        roadmap_id=roadmap_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
