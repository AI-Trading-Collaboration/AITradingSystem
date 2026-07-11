from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    DEFAULT_DATA_PROVENANCE_DIR,
    DEFAULT_LATEST_POINTER_DIR,
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DEFAULT_PROMOTION_DIR,
    DEFAULT_SCHEDULE_OBSERVE_DIR,
    DEFAULT_SHADOW_MONITOR_DIR,
    DEFAULT_SHADOW_REGISTRY_PATH,
    DEFAULT_WINDOW_AUDIT_DIR,
    DynamicV3ParameterResearchError,
    build_promotion_pack,
    promotion_review_payload,
    run_shadow_monitor,
    scheduled_observe_payload,
    shadow_monitor_report_payload,
    validate_promotion_pack,
    validate_shadow_monitor_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj, parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_promotion_app,
    dynamic_v3_rescue_app,
    dynamic_v3_schedule_app,
    dynamic_v3_shadow_app,
)


@dynamic_v3_shadow_app.command("monitor-run")
def dynamic_v3_shadow_monitor_run_command(
    as_of: Annotated[str, typer.Option("--as-of", help="shadow monitor as-of date。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_DIR,
) -> None:
    """运行 TRADING-110 shadow monitor。"""
    result = run_shadow_monitor(
        as_of=parse_date(as_of),
        registry_path=registry_path,
        output_dir=output_dir,
    )
    report = result["report"]
    summary = mapping_obj(report.get("summary"))
    typer.echo(f"monitor_id={result['monitor_id']}")
    typer.echo(f"monitor_dir={result['monitor_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"observe_only_candidate_count={summary.get('observe_only_candidate_count')}")
    typer.echo(f"promotion_review_ready_count={summary.get('promotion_review_ready_count')}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_app.command("monitor-report")
def dynamic_v3_shadow_monitor_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shadow monitor。"),
    ] = False,
    monitor_id: Annotated[str | None, typer.Option("--monitor-id", help="monitor id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_DIR,
) -> None:
    """展示 TRADING-110 shadow monitor report。"""
    payload = shadow_monitor_report_payload(
        monitor_id=monitor_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"monitor_id={payload['monitor_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shadow-monitor")
def dynamic_v3_validate_shadow_monitor_command(
    monitor_id: Annotated[str, typer.Option("--monitor-id", help="monitor id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_DIR,
) -> None:
    """校验 TRADING-110 shadow monitor artifacts。"""
    payload = validate_shadow_monitor_artifact(monitor_id=monitor_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_schedule_app.command("observe")
def dynamic_v3_schedule_observe_command(
    as_of: Annotated[str, typer.Option("--as-of", help="scheduled observation as-of date。")],
    family: Annotated[
        str,
        typer.Option("--family", help="artifact family。"),
    ] = "dynamic_v3_rescue",
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="parameter sweep config。"),
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    pointer_dir: Annotated[
        Path,
        typer.Option("--pointer-dir", help="latest pointer directory。"),
    ] = DEFAULT_LATEST_POINTER_DIR,
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="scheduled observe artifact root。"),
    ] = DEFAULT_SCHEDULE_OBSERVE_DIR,
    run_shadow_monitor: Annotated[
        bool,
        typer.Option(
            "--run-shadow-monitor/--skip-shadow-monitor",
            help="Run observe-only shadow monitor when weekly due conditions pass。",
        ),
    ] = True,
    force_due: Annotated[
        bool,
        typer.Option("--force-due", help="Force due=true for manual validation only。"),
    ] = False,
) -> None:
    """运行 TRADING-099 daily scheduler lightweight observe gate。"""
    if not as_of:
        raise typer.BadParameter("--as-of is required")
    try:
        observation_date = date.fromisoformat(as_of)
    except ValueError as exc:
        raise typer.BadParameter("--as-of must use YYYY-MM-DD") from exc
    payload = scheduled_observe_payload(
        as_of=observation_date,
        family=family,
        config_path=config_path,
        pointer_dir=pointer_dir,
        registry_path=registry_path,
        output_dir=output_dir,
        run_shadow_monitor_on_due=run_shadow_monitor,
        force_due=force_due,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"due_status={payload['due_status']}")
    typer.echo(f"pointer_count={payload['pointer_count']}")
    typer.echo(f"json={payload['output_artifacts']['json']}")
    typer.echo(f"markdown={payload['output_artifacts']['markdown']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] == "FAIL":
        raise typer.Exit(code=1)


@dynamic_v3_promotion_app.command("review")
def dynamic_v3_promotion_review_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
) -> None:
    """展示 TRADING-100 promotion review readiness。"""
    payload = promotion_review_payload(candidate_id=candidate_id, registry_path=registry_path)
    typer.echo(f"candidate_id={candidate_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"registry_record_present={payload['registry_record_present']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_promotion_app.command("pack")
def dynamic_v3_promotion_pack_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", "--registry-path", help="shadow registry path。"),
    ] = DEFAULT_SHADOW_REGISTRY_PATH,
    candidate_attribution_dir: Annotated[
        Path,
        typer.Option("--candidate-attribution-dir", help="candidate attribution artifact root。"),
    ] = DEFAULT_CANDIDATE_ATTRIBUTION_DIR,
    data_provenance_dir: Annotated[
        Path,
        typer.Option("--data-provenance-dir", help="data provenance artifact root。"),
    ] = DEFAULT_DATA_PROVENANCE_DIR,
    window_audit_dir: Annotated[
        Path,
        typer.Option("--window-audit-dir", help="window audit artifact root。"),
    ] = DEFAULT_WINDOW_AUDIT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion artifact root。"),
    ] = DEFAULT_PROMOTION_DIR,
) -> None:
    """生成 TRADING-100 promotion review pack。"""
    try:
        result = build_promotion_pack(
            candidate_id=candidate_id,
            registry_path=registry_path,
            candidate_attribution_dir=candidate_attribution_dir,
            data_provenance_dir=data_provenance_dir,
            window_audit_dir=window_audit_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    pack = result["pack"]
    typer.echo(f"promotion_id={result['promotion_id']}")
    typer.echo(f"promotion_dir={result['promotion_dir']}")
    typer.echo(f"status={pack['status']}")
    typer.echo("manual_review_required=true")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-promotion-pack")
def dynamic_v3_validate_promotion_pack_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="promotion artifact root。"),
    ] = DEFAULT_PROMOTION_DIR,
) -> None:
    """校验 TRADING-100 promotion pack。"""
    payload = validate_promotion_pack(candidate_id=candidate_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
