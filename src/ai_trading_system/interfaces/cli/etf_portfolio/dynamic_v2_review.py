from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_rescue import DEFAULT_DYNAMIC_RESCUE_REPORT_DIR
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_shadow import DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR
from ai_trading_system.etf_portfolio.dynamic_v2_review import (
    DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
    DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_V2_REVIEW_VALIDATION_DIR,
    DynamicV2ReviewError,
    build_dynamic_v2_review_package,
    build_dynamic_v2_review_validation_report,
    latest_dynamic_v2_review_package_path,
    load_dynamic_v2_review_policy_config,
    load_latest_review_inputs,
    write_dynamic_v2_review_package,
    write_dynamic_v2_review_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload,
    mapping_obj,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_v2_review_app


@dynamic_v2_review_app.command("package")
def dynamic_v2_review_package_command(
    latest_rescue_report: Annotated[
        bool,
        typer.Option(
            "--latest-rescue-report/--no-latest-rescue-report",
            help="读取 latest TRADING-088 dynamic rescue report。",
        ),
    ] = True,
    rescue_report_path: Annotated[
        Path | None,
        typer.Option("--dynamic-rescue-report", "--rescue-report", help="TRADING-088 JSON。"),
    ] = None,
    candidate_robustness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--candidate-robustness-report",
            "--dynamic-robustness-report",
            help="v0.4 candidate TRADING-086 robustness JSON。",
        ),
    ] = None,
    dynamic_shadow_package_path: Annotated[
        Path | None,
        typer.Option("--dynamic-shadow-package", help="optional TRADING-087 package JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic v0.2 review policy config。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    rescue_report_dir: Annotated[
        Path,
        typer.Option("--rescue-report-dir", help="dynamic rescue report 目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
    candidate_robustness_report_dir: Annotated[
        Path,
        typer.Option("--candidate-robustness-report-dir", help="dynamic robustness report 目录。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    dynamic_shadow_package_dir: Annotated[
        Path,
        typer.Option("--dynamic-shadow-package-dir", help="dynamic shadow package 目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dynamic v0.2 review package 输出目录。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
) -> None:
    """生成 TRADING-089 v0.4 review-only package；不 enroll、不 approval。"""
    if not latest_rescue_report and rescue_report_path is None:
        raise typer.BadParameter("--dynamic-rescue-report or --latest-rescue-report is required")
    try:
        rescue_report, robustness_report, shadow_package, source_paths = load_latest_review_inputs(
            rescue_report_path=rescue_report_path,
            candidate_robustness_report_path=candidate_robustness_report_path,
            shadow_package_path=dynamic_shadow_package_path,
            rescue_report_dir=rescue_report_dir,
            candidate_robustness_report_dir=candidate_robustness_report_dir,
            shadow_package_dir=dynamic_shadow_package_dir,
        )
        payload = build_dynamic_v2_review_package(
            rescue_report=rescue_report,
            candidate_robustness_report=robustness_report,
            shadow_package=shadow_package,
            policy=load_dynamic_v2_review_policy_config(config_path),
            source_paths=source_paths,
        )
    except DynamicV2ReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_v2_review_package(payload, output_dir=output_dir)
    gate = mapping_obj(payload.get("shadow_review_eligibility_gate"))
    typer.echo(f"ETF dynamic v0.2 review package JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.2 review package Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"review_status={payload['review_status']}")
    typer.echo(f"candidate={mapping_obj(payload.get('candidate_evidence')).get('candidate_id')}")
    typer.echo(f"blockers={','.join(str(item) for item in gate.get('blocking_reason_codes', []))}")
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


@dynamic_v2_review_app.command("report")
def dynamic_v2_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest dynamic v0.2 review package。"),
    ] = True,
    package_output_dir: Annotated[
        Path,
        typer.Option("--package-output-dir", help="dynamic v0.2 review package 目录。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_PACKAGE_DIR,
) -> None:
    """只读展示 latest TRADING-089 dynamic v0.2 review package。"""
    if not latest:
        raise typer.BadParameter("dynamic-v2-review report currently supports --latest")
    resolved = latest_dynamic_v2_review_package_path(package_output_dir)
    payload = load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic v0.2 review package not found")
    gate = mapping_obj(payload.get("shadow_review_eligibility_gate"))
    typer.echo(f"dynamic_v2_review_package={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"review_status={payload.get('review_status')}")
    typer.echo(f"blockers={','.join(str(item) for item in gate.get('blocking_reason_codes', []))}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_v2_review_app.command("validate")
def dynamic_v2_review_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic v0.2 review policy config。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_V2_REVIEW_VALIDATION_DIR,
) -> None:
    """校验 TRADING-089 dynamic v0.2 review-only workflow 和 safety boundary。"""
    payload = build_dynamic_v2_review_validation_report(config_path=config_path)
    paths = write_dynamic_v2_review_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic v0.2 review validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.2 review validation Markdown：{paths['markdown']}")
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
