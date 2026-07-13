from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DEFAULT_CONFIRMATION_REGISTRY_DIR,
    DEFAULT_CONFIRMATION_REGISTRY_YAML_PATH,
    confirmation_targets_report_payload,
    list_confirmation_targets,
    register_confirmation_targets,
    validate_confirmation_targets_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_confirmation_targets_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_confirmation_targets_app.command("register")
def dynamic_v3_confirmation_targets_register_command(
    confirmation_plan_id: Annotated[
        str,
        typer.Option(
            "--confirmation-plan-id",
            "--confirmation_plan_id",
            help="forward confirmation plan id。",
        ),
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="confirmation registry artifact root。")
    ] = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    confirmation_plan_dir: Annotated[
        Path, typer.Option("--confirmation-plan-dir", help="forward confirmation plan root。")
    ] = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
    registry_yaml_path: Annotated[
        Path, typer.Option("--registry-yaml-path", help="reviewable registry YAML path。")
    ] = DEFAULT_CONFIRMATION_REGISTRY_YAML_PATH,
) -> None:
    """注册 TRADING-174 forward confirmation targets。"""
    result = register_confirmation_targets(
        confirmation_plan_id=confirmation_plan_id,
        confirmation_plan_dir=confirmation_plan_dir,
        output_dir=output_dir,
        registry_yaml_path=registry_yaml_path,
    )
    manifest = result["manifest"]
    typer.echo(f"registry_id={result['registry_id']}")
    typer.echo(f"registry_dir={result['registry_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"target_count={manifest['targets_total']}")
    typer.echo(f"active_target_count={manifest['active_target_count']}")
    typer.echo(f"watch_only_target_count={manifest['watch_only_target_count']}")
    typer.echo("auto_apply=false")
    typer.echo("owner_approval_required=true")
    typer.echo("production_effect=none")


@dynamic_v3_confirmation_targets_app.command("list")
def dynamic_v3_confirmation_targets_list_command(
    registry_id: Annotated[
        str | None, typer.Option("--registry-id", "--registry_id", help="registry id。")
    ] = None,
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest registry。")
    ] = True,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="confirmation registry artifact root。")
    ] = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> None:
    """列出 TRADING-174 confirmation targets。"""
    payload = list_confirmation_targets(
        registry_id=registry_id, latest=latest, output_dir=output_dir
    )
    typer.echo(f"registry_id={payload['registry_id']}")
    typer.echo(f"target_count={payload['targets_total']}")
    typer.echo(f"active_target_count={payload['active_target_count']}")
    typer.echo(f"watch_only_target_count={payload['watch_only_target_count']}")
    for target in payload["targets"]:
        typer.echo(
            "target="
            + json.dumps(
                {
                    "target_id": target.get("target_id"),
                    "status": target.get("status"),
                    "current_status": target.get("current_status"),
                    "auto_apply": target.get("auto_apply"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    typer.echo("production_effect=none")


@dynamic_v3_confirmation_targets_app.command("report")
def dynamic_v3_confirmation_targets_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest registry。")
    ] = False,
    registry_id: Annotated[
        str | None, typer.Option("--registry-id", "--registry_id", help="registry id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="confirmation registry artifact root。")
    ] = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> None:
    """展示 TRADING-174 confirmation target registry 摘要。"""
    payload = confirmation_targets_report_payload(
        registry_id=registry_id, latest=latest, output_dir=output_dir
    )
    typer.echo(f"registry_id={payload['registry_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"target_count={payload['targets_total']}")
    typer.echo(f"active_target_count={payload['active_target_count']}")
    typer.echo(f"watch_only_target_count={payload['watch_only_target_count']}")
    typer.echo(f"report_path={payload['confirmation_targets_report_path']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-confirmation-targets")
def dynamic_v3_validate_confirmation_targets_command(
    registry_id: Annotated[
        str, typer.Option("--registry-id", "--registry_id", help="registry id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="confirmation registry artifact root。")
    ] = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> None:
    """校验 TRADING-174 confirmation target registry artifact。"""
    payload = validate_confirmation_targets_artifact(
        registry_id=registry_id, output_dir=output_dir
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_confirmation_targets_list_command",
    "dynamic_v3_confirmation_targets_register_command",
    "dynamic_v3_confirmation_targets_report_command",
    "dynamic_v3_validate_confirmation_targets_command",
]
