from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v2_review import (
    DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR,
    DEFAULT_DYNAMIC_V3_RESCUE_VALIDATION_DIR,
    DynamicV3RescueError,
    build_dynamic_v3_rescue_evaluation_report,
    build_dynamic_v3_rescue_validation_report,
    latest_dynamic_v3_rescue_report_path,
    load_dynamic_v3_rescue_policy_config,
    load_latest_v3_rescue_inputs,
    write_dynamic_v3_rescue_evaluation_report,
    write_dynamic_v3_rescue_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload,
    mapping_obj,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_v3_rescue_app


@dynamic_v3_rescue_app.command("run")
def dynamic_v3_rescue_run_command(
    latest_v2_review: Annotated[
        bool,
        typer.Option(
            "--latest-v2-review/--no-latest-v2-review",
            help="读取 latest TRADING-089 dynamic v0.2 review package。",
        ),
    ] = True,
    v2_review_package_path: Annotated[
        Path | None,
        typer.Option(
            "--v2-review-package",
            "--v0-4-review-package",
            help="Explicit TRADING-089 v0.4 review package JSON。",
        ),
    ] = None,
    base_candidate: Annotated[
        str | None,
        typer.Option(
            "--base-candidate",
            help="Base candidate policy id；默认 v0.4 lower_turnover。",
        ),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic v0.3 rescue policy config。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    v2_review_package_dir: Annotated[
        Path,
        typer.Option("--v2-review-package-dir", help="dynamic v0.2 review package 目录。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", "--report-output-dir", help="dynamic v0.3 rescue 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR,
) -> None:
    """生成 TRADING-090 v0.3 constraint-aware rescue evaluation report；不 enroll。"""
    if not latest_v2_review and v2_review_package_path is None:
        raise typer.BadParameter("--v2-review-package or --latest-v2-review is required")
    try:
        policy = load_dynamic_v3_rescue_policy_config(config_path)
        if base_candidate is not None and base_candidate != policy.base_candidate:
            raise DynamicV3RescueError(
                f"TRADING-090 base candidate must be {policy.base_candidate}"
            )
        v2_package, source_paths = load_latest_v3_rescue_inputs(
            v2_review_package_path=v2_review_package_path,
            v2_review_package_dir=v2_review_package_dir,
        )
        payload = build_dynamic_v3_rescue_evaluation_report(
            v04_review_package=v2_package,
            policy=policy,
            v04_review_package_path=source_paths.get("v0_4_review_package"),
        )
    except DynamicV3RescueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_v3_rescue_evaluation_report(payload, output_dir=output_dir)
    best = mapping_obj(payload.get("best_candidate"))
    typer.echo(f"ETF dynamic v0.3 rescue report JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 rescue report Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"review_status={payload['review_status']}")
    typer.echo(f"best_candidate={best.get('policy_id')}")
    typer.echo(f"best_candidate_status={best.get('candidate_status')}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_v3_rescue_app.command("report")
def dynamic_v3_rescue_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest dynamic v0.3 rescue report。"),
    ] = True,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic v0.3 rescue report 目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-090 dynamic v0.3 rescue report。"""
    if not latest:
        raise typer.BadParameter("dynamic-v3-rescue report currently supports --latest")
    resolved = latest_dynamic_v3_rescue_report_path(report_output_dir)
    payload = load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic v0.3 rescue report not found")
    best = mapping_obj(payload.get("best_candidate"))
    typer.echo(f"dynamic_v3_rescue_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"review_status={payload.get('review_status')}")
    typer.echo(f"best_candidate={best.get('policy_id')}")
    typer.echo(f"best_candidate_status={best.get('candidate_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_v3_rescue_app.command("validate")
def dynamic_v3_rescue_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic v0.3 rescue policy config。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_VALIDATION_DIR,
) -> None:
    """校验 TRADING-090 dynamic v0.3 rescue workflow 和 safety boundary。"""
    payload = build_dynamic_v3_rescue_validation_report(config_path=config_path)
    paths = write_dynamic_v3_rescue_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic v0.3 rescue validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 rescue validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
