from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_bootstrap as system_target,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    run_cached_data_quality_gate as _run_cached_data_quality_gate,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_smoothed_daily_emission_app,
    dynamic_v3_smoothed_forward_classify_app,
    dynamic_v3_smoothed_forward_weekly_run_app,
    dynamic_v3_smoothed_outcome_due_app,
    dynamic_v3_smoothed_outcome_update_app,
)


def _records_obj(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in value] if isinstance(value, list) else []


@dynamic_v3_smoothed_daily_emission_app.command("run")
def dynamic_v3_smoothed_daily_emission_run_command(
    as_of: Annotated[str, typer.Option("--as-of", help="emission as_of。")],
    target_id: Annotated[
        str | None,
        typer.Option("--target-id", help="可选 model target id；默认读取 latest。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed daily emission artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
) -> None:
    """生成 TRADING-271 smoothed daily forward observation event。"""
    emission_as_of = _parse_date(as_of)
    _run_cached_data_quality_gate(
        prices_path=price_cache_path,
        rates_path=rates_path,
        as_of=emission_as_of,
        output_path=data_quality_output_path,
    )
    result = system_target.run_smoothed_daily_emission(
        as_of=emission_as_of,
        target_id=target_id,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_path,
    )
    manifest = result["manifest"]
    quality = result["smoothed_emission_data_quality"]
    typer.echo(f"emission_id={result['emission_id']}")
    typer.echo(f"emission_dir={result['emission_dir']}")
    typer.echo(f"emitted_event_count={manifest['emitted_event_count']}")
    typer.echo(f"as_of={manifest['as_of']}")
    typer.echo(f"event_status={manifest['event_status']}")
    typer.echo(f"data_quality={quality['data_quality']}")
    typer.echo("future_data_used=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_daily_emission_app.command("report")
def dynamic_v3_smoothed_daily_emission_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest daily emission。"),
    ] = False,
    emission_id: Annotated[
        str | None,
        typer.Option("--emission-id", help="emission id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed daily emission artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
) -> None:
    """展示 TRADING-271 smoothed daily emission 摘要。"""
    payload = system_target.smoothed_daily_emission_report_payload(
        emission_id=emission_id,
        latest=latest,
        output_dir=output_dir,
    )
    events = _records_obj(payload.get("smoothed_forward_events"))
    event = events[0] if events else {}
    typer.echo(f"emission_id={payload['emission_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"emitted_event_count={payload['emitted_event_count']}")
    typer.echo(f"as_of={payload['as_of']}")
    typer.echo(f"event_status={event.get('event_status')}")
    typer.echo(f"data_quality={payload['data_quality']}")
    typer.echo(f"report_path={payload['smoothed_daily_emission_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-daily-emission")
def dynamic_v3_validate_smoothed_daily_emission_command(
    emission_id: Annotated[str, typer.Option("--emission-id", help="emission id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed daily emission artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
) -> None:
    """校验 TRADING-271 smoothed daily emission artifact。"""
    payload = system_target.validate_smoothed_daily_emission_artifact(
        emission_id=emission_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("future_data_used=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_outcome_due_app.command("scan")
def dynamic_v3_smoothed_outcome_due_scan_command(
    as_of: Annotated[str, typer.Option("--as-of", help="scanner as_of。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed outcome due artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
) -> None:
    """扫描 TRADING-272 smoothed forward outcome due windows。"""
    scanner_as_of = _parse_date(as_of)
    _run_cached_data_quality_gate(
        prices_path=price_cache_path,
        rates_path=rates_path,
        as_of=scanner_as_of,
        output_path=data_quality_output_path,
    )
    result = system_target.scan_smoothed_outcome_due(
        as_of=scanner_as_of,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_path,
    )
    summary = result["due_summary"]
    typer.echo(f"due_id={result['due_id']}")
    typer.echo(f"due_dir={result['due_dir']}")
    typer.echo(f"events_scanned={summary['events_scanned']}")
    typer.echo(f"due_windows={summary['due_windows']}")
    typer.echo(f"update_ready_count={summary['update_ready_count']}")
    typer.echo(f"blocked_future_as_of={summary['blocked_future_as_of']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_outcome_due_app.command("report")
def dynamic_v3_smoothed_outcome_due_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest due scan。"),
    ] = False,
    due_id: Annotated[str | None, typer.Option("--due-id", help="due id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed outcome due artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
) -> None:
    """展示 TRADING-272 due scanner 摘要。"""
    payload = system_target.smoothed_outcome_due_report_payload(
        due_id=due_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("due_summary"))
    typer.echo(f"due_id={payload['due_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"events_scanned={summary.get('events_scanned')}")
    typer.echo(f"due_windows={summary.get('due_windows')}")
    typer.echo(f"update_ready_count={summary.get('update_ready_count')}")
    typer.echo(f"blocked_future_as_of={summary.get('blocked_future_as_of')}")
    typer.echo(f"report_path={payload['smoothed_outcome_due_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-outcome-due")
def dynamic_v3_validate_smoothed_outcome_due_command(
    due_id: Annotated[str, typer.Option("--due-id", help="due id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed outcome due artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
) -> None:
    """校验 TRADING-272 smoothed outcome due artifact。"""
    payload = system_target.validate_smoothed_outcome_due_artifact(
        due_id=due_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_outcome_update_app.command("run")
def dynamic_v3_smoothed_outcome_update_run_command(
    due_id: Annotated[str, typer.Option("--due-id", help="due id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed outcome update artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
) -> None:
    """运行 TRADING-273 smoothed forward outcome updater。"""
    due_payload = system_target.smoothed_outcome_due_report_payload(due_id=due_id)
    due_summary = _mapping_obj(due_payload.get("due_summary"))
    scanner_as_of = _parse_date(str(due_summary.get("scanner_as_of") or date.today()))
    _run_cached_data_quality_gate(
        prices_path=price_cache_path,
        rates_path=rates_path,
        as_of=scanner_as_of,
        output_path=data_quality_output_path,
    )
    result = system_target.run_smoothed_outcome_update(
        due_id=due_id,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_path,
    )
    summary = result["smoothed_outcome_delta_summary"]
    typer.echo(f"update_id={result['update_id']}")
    typer.echo(f"update_dir={result['update_dir']}")
    typer.echo(f"updated_windows={summary['updated_count']}")
    typer.echo(f"skipped_windows={summary['skipped_count']}")
    typer.echo(
        f"available_forward_events_after_update={summary['available_forward_events_after_update']}"
    )
    typer.echo("future_data_used=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_outcome_update_app.command("report")
def dynamic_v3_smoothed_outcome_update_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest outcome update。"),
    ] = False,
    update_id: Annotated[str | None, typer.Option("--update-id", help="update id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed outcome update artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
) -> None:
    """展示 TRADING-273 outcome updater 摘要。"""
    payload = system_target.smoothed_outcome_update_report_payload(
        update_id=update_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("smoothed_outcome_delta_summary"))
    typer.echo(f"update_id={payload['update_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"updated_windows={summary.get('updated_count')}")
    typer.echo(f"skipped_windows={summary.get('skipped_count')}")
    typer.echo(
        "available_forward_events_after_update="
        f"{summary.get('available_forward_events_after_update')}"
    )
    typer.echo(f"report_path={payload['smoothed_outcome_update_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-outcome-update")
def dynamic_v3_validate_smoothed_outcome_update_command(
    update_id: Annotated[str, typer.Option("--update-id", help="update id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed outcome update artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
) -> None:
    """校验 TRADING-273 smoothed outcome update artifact。"""
    payload = system_target.validate_smoothed_outcome_update_artifact(
        update_id=update_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("future_data_used=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_forward_classify_app.command("run")
def dynamic_v3_smoothed_forward_classify_run_command(
    update_id: Annotated[str, typer.Option("--update-id", help="update id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed classification artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
) -> None:
    """运行 TRADING-274 sideways/recovery forward classifier。"""
    result = system_target.run_smoothed_forward_classification(
        update_id=update_id,
        output_dir=output_dir,
    )
    summary = result["classification_summary"]
    typer.echo(f"classification_id={result['classification_id']}")
    typer.echo(f"classification_dir={result['classification_dir']}")
    typer.echo(f"events_classified={summary['events_classified']}")
    typer.echo(f"sideways_events={summary['sideways_events_available']}")
    typer.echo(f"recovery_events={summary['recovery_events_available']}")
    typer.echo(f"lag_warning_count={summary['lag_warning_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_forward_classify_app.command("report")
def dynamic_v3_smoothed_forward_classify_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest classification。"),
    ] = False,
    classification_id: Annotated[
        str | None,
        typer.Option("--classification-id", help="classification id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed classification artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
) -> None:
    """展示 TRADING-274 classifier 摘要。"""
    payload = system_target.smoothed_forward_classification_report_payload(
        classification_id=classification_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("classification_summary"))
    typer.echo(f"classification_id={payload['classification_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"events_classified={summary.get('events_classified')}")
    typer.echo(f"sideways_events={summary.get('sideways_events_available')}")
    typer.echo(f"recovery_events={summary.get('recovery_events_available')}")
    typer.echo(f"lag_warning_count={summary.get('lag_warning_count')}")
    typer.echo(f"report_path={payload['smoothed_forward_classification_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-forward-classify")
def dynamic_v3_validate_smoothed_forward_classify_command(
    classification_id: Annotated[
        str,
        typer.Option("--classification-id", help="classification id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed classification artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
) -> None:
    """校验 TRADING-274 smoothed forward classification artifact。"""
    payload = system_target.validate_smoothed_forward_classification_artifact(
        classification_id=classification_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_forward_weekly_run_app.command("run")
def dynamic_v3_smoothed_forward_weekly_run_command(
    week_ending: Annotated[str, typer.Option("--week-ending", help="week ending date。")],
    target_id: Annotated[
        str | None,
        typer.Option("--target-id", help="可选 model target id；默认读取 latest。"),
    ] = None,
    binding_id: Annotated[
        str | None,
        typer.Option("--binding-id", help="smoothed forward binding id。"),
    ] = None,
    switch_plan_id: Annotated[
        str | None,
        typer.Option("--switch-plan-id", help="paper shadow switch plan id。"),
    ] = None,
    owner_promotion_id: Annotated[
        str | None,
        typer.Option("--owner-promotion-id", help="owner promotion id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed weekly run artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
) -> None:
    """运行 TRADING-275 smoothed forward weekly evidence runner。"""
    week_ending_date = _parse_date(week_ending)
    _run_cached_data_quality_gate(
        prices_path=price_cache_path,
        rates_path=rates_path,
        as_of=week_ending_date,
        output_path=data_quality_output_path,
    )
    result = system_target.run_smoothed_forward_weekly_run(
        week_ending=week_ending_date,
        target_id=target_id,
        binding_id=binding_id,
        switch_plan_id=switch_plan_id,
        owner_promotion_id=owner_promotion_id,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_path,
    )
    summary = result["weekly_run_summary"]
    typer.echo(f"weekly_run_id={result['weekly_run_id']}")
    typer.echo(f"weekly_run_dir={result['weekly_run_dir']}")
    typer.echo(f"emitted_events={summary['emitted_events']}")
    typer.echo(f"updated_windows={summary['updated_windows']}")
    typer.echo(
        "forward_progress="
        f"{summary['available_forward_events']}/{summary['required_forward_events']}"
    )
    typer.echo(
        "sideways_progress="
        f"{summary['available_sideways_events']}/{summary['required_sideways_events']}"
    )
    typer.echo(
        "recovery_progress="
        f"{summary['available_recovery_events']}/{summary['required_recovery_events']}"
    )
    typer.echo(f"can_execute_switch={summary['can_execute_switch']}")
    typer.echo(f"weekly_recommendation={summary['weekly_recommendation']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_forward_weekly_run_app.command("report")
def dynamic_v3_smoothed_forward_weekly_run_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest weekly run。"),
    ] = False,
    weekly_run_id: Annotated[
        str | None,
        typer.Option("--weekly-run-id", help="weekly run id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed weekly run artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
) -> None:
    """展示 TRADING-275 weekly runner 摘要。"""
    payload = system_target.smoothed_forward_weekly_run_report_payload(
        weekly_run_id=weekly_run_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("weekly_run_summary"))
    typer.echo(f"weekly_run_id={payload['weekly_run_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"emitted_events={summary.get('emitted_events')}")
    typer.echo(f"updated_windows={summary.get('updated_windows')}")
    typer.echo(
        "forward_progress="
        f"{summary.get('available_forward_events')}/{summary.get('required_forward_events')}"
    )
    typer.echo(f"can_execute_switch={summary.get('can_execute_switch')}")
    typer.echo(f"weekly_recommendation={summary.get('weekly_recommendation')}")
    typer.echo(f"report_path={payload['smoothed_forward_weekly_run_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-forward-weekly-run")
def dynamic_v3_validate_smoothed_forward_weekly_run_command(
    weekly_run_id: Annotated[
        str,
        typer.Option("--weekly-run-id", help="weekly run id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed weekly run artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
) -> None:
    """校验 TRADING-275 smoothed forward weekly run artifact。"""
    payload = system_target.validate_smoothed_forward_weekly_run_artifact(
        weekly_run_id=weekly_run_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("can_execute_switch=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
