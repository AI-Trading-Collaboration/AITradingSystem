from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_calibration import (
    DEFAULT_DYNAMIC_CALIBRATION_CANDIDATE_DIR,
    DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR,
    DEFAULT_DYNAMIC_CALIBRATION_VALIDATION_DIR,
    DynamicCalibrationError,
    build_dynamic_calibration_batch_report,
    build_dynamic_calibration_validation_report,
    latest_dynamic_calibration_report_path,
    load_dynamic_calibration_policy_config,
    load_latest_trend_report,
    write_dynamic_calibration_candidate_packs,
    write_dynamic_calibration_report,
    write_dynamic_calibration_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload,
    mapping_obj,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_calibration_app


@dynamic_calibration_app.command("run")
def dynamic_calibration_run_command(
    pack: Annotated[
        str | None,
        typer.Option("--pack", help="Two-layer dynamic candidate pack id。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic calibration policy config。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    trend_report_path: Annotated[
        Path | None,
        typer.Option("--trend-report-path", help="Explicit TRADING-083 trend report JSON。"),
    ] = None,
    latest_trend_report: Annotated[
        bool,
        typer.Option(
            "--latest-trend-report/--no-latest-trend-report",
            help="没有显式 trend report 时读取 latest TRADING-083 report。",
        ),
    ] = True,
    cache: Annotated[
        str | None,
        typer.Option("--cache", help="Cache mode: read-write/read-only/disabled。"),
    ] = None,
    cache_root: Annotated[
        Path | None,
        typer.Option("--cache-root", help="dynamic calibration cache root。"),
    ] = None,
    workers: Annotated[
        str | None,
        typer.Option("--workers", help="Worker count or auto。"),
    ] = None,
    top: Annotated[
        int | None,
        typer.Option("--top", help="Top dynamic candidate packs to show。"),
    ] = None,
    candidate_output_dir: Annotated[
        Path,
        typer.Option("--candidate-output-dir", help="candidate pack 输出目录。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_CANDIDATE_DIR,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic calibration report 输出目录。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR,
) -> None:
    """运行 TRADING-085 two-layer dynamic candidate batch/cache；不写 production weights。"""
    if cache not in {None, "read-write", "read-only", "disabled"}:
        raise typer.BadParameter("--cache must be read-write, read-only, or disabled")
    try:
        policy = load_dynamic_calibration_policy_config(config_path)
        dynamic_policy = load_dynamic_allocation_policy_config(dynamic_allocation_config_path)
        resolved_trend_report_path = trend_report_path
        trend_payload: dict[str, Any] = {}
        if latest_trend_report or trend_report_path is not None:
            resolved_trend_report_path, trend_payload = load_latest_trend_report(trend_report_path)
        report = build_dynamic_calibration_batch_report(
            policy=policy,
            dynamic_policy=dynamic_policy,
            trend_report=trend_payload,
            trend_report_path=resolved_trend_report_path,
            dynamic_policy_path=dynamic_allocation_config_path,
            pack_id=pack,
            cache_mode=cache,
            cache_root=cache_root,
            workers=workers,
            top_n=top,
        )
    except DynamicCalibrationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    candidate_paths = write_dynamic_calibration_candidate_packs(
        report,
        output_dir=candidate_output_dir,
    )
    report_paths = write_dynamic_calibration_report(report, output_dir=report_output_dir)
    summary = mapping_obj(report.get("summary"))
    cache_summary = mapping_obj(report.get("cache_summary"))
    typer.echo(f"ETF dynamic calibration candidate packs JSON：{candidate_paths['json']}")
    typer.echo(f"ETF dynamic calibration report JSON：{report_paths['json']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"candidate_pack_count={report['candidate_pack_count']}")
    typer.echo(f"top_candidate={summary.get('top_dynamic_candidate_pack_id')}")
    typer.echo(f"top_ranking_score={summary.get('top_ranking_score')}")
    typer.echo(f"cache_hit_rate={cache_summary.get('cache_hit_rate')}")
    typer.echo(f"cache_write_count={cache_summary.get('cache_write_count')}")
    typer.echo("calibration_proxy=true")
    typer.echo("full_robustness_backtest_required=true")
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


@dynamic_calibration_app.command("report")
def dynamic_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取最新 dynamic calibration report。"),
    ] = True,
    report_path: Annotated[
        Path | None,
        typer.Option("--report-path", help="显式 report JSON path。"),
    ] = None,
    report_dir: Annotated[
        Path,
        typer.Option("--report-dir", help="report artifact directory。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-085 dynamic calibration report 摘要。"""
    resolved = report_path
    if resolved is None and latest:
        resolved = latest_dynamic_calibration_report_path(report_dir)
    payload = load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic calibration report not found")
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise typer.BadParameter(f"invalid dynamic calibration report: {resolved}")
    cache_summary = mapping_obj(payload.get("cache_summary"))
    typer.echo(f"dynamic_calibration_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"candidate_pack_count={payload.get('candidate_pack_count')}")
    typer.echo(f"top_candidate={summary.get('top_dynamic_candidate_pack_id')}")
    typer.echo(f"top_ranking_score={summary.get('top_ranking_score')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo(f"cache_hit_rate={cache_summary.get('cache_hit_rate')}")
    typer.echo("calibration_proxy=true")
    typer.echo("full_robustness_backtest_required=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("official_target_weights_mutated=false")


@dynamic_calibration_app.command("validate")
def dynamic_calibration_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic calibration policy config。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_CALIBRATION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-085 dynamic calibration workflow 和 safety boundary。"""
    payload = build_dynamic_calibration_validation_report(
        policy_config_path=config_path,
        dynamic_policy_path=dynamic_allocation_config_path,
    )
    paths = write_dynamic_calibration_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic calibration validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic calibration validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
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
