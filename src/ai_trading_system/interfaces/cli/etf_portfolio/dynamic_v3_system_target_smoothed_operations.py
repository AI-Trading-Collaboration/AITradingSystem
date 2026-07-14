from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_operations as system_target,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_smoothed_event_monitor_app,
    dynamic_v3_smoothed_forward_progress_app,
    dynamic_v3_smoothed_owner_renewal_app,
    dynamic_v3_smoothed_switch_readiness_app,
    dynamic_v3_smoothed_weekly_dashboard_app,
)


def _mapping_obj(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records_obj(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


@dynamic_v3_smoothed_forward_progress_app.command("update")
def dynamic_v3_smoothed_forward_progress_update_command(
    binding_id: Annotated[str, typer.Option("--binding-id", help="smoothed binding id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed forward progress artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
) -> None:
    """运行 TRADING-266 smoothed forward progress tracker。"""
    result = system_target.update_smoothed_forward_progress(
        binding_id=binding_id,
        output_dir=output_dir,
    )
    summary = result["smoothed_forward_progress_summary"]
    typer.echo(f"progress_id={result['progress_id']}")
    typer.echo(f"progress_dir={result['progress_dir']}")
    typer.echo(
        "forward_events="
        f"{summary['available_forward_events_total']}/"
        f"{summary['required_forward_events_total']}"
    )
    typer.echo(
        "sideways_events="
        f"{summary['available_sideways_events']}/{summary['required_sideways_events']}"
    )
    typer.echo(
        "recovery_events="
        f"{summary['available_recovery_events']}/{summary['required_recovery_events']}"
    )
    typer.echo(f"summary_recommendation={summary['summary_recommendation']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_forward_progress_app.command("report")
def dynamic_v3_smoothed_forward_progress_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest smoothed progress。"),
    ] = False,
    progress_id: Annotated[
        str | None,
        typer.Option("--progress-id", help="progress id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed forward progress artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
) -> None:
    """展示 TRADING-266 smoothed forward progress 摘要。"""
    payload = system_target.smoothed_forward_progress_report_payload(
        progress_id=progress_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("smoothed_forward_progress_summary"))
    typer.echo(f"progress_id={payload['progress_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        "forward_events="
        f"{summary.get('available_forward_events_total')}/"
        f"{summary.get('required_forward_events_total')}"
    )
    typer.echo(f"ready_for_review_count={summary.get('ready_for_review_count')}")
    typer.echo(f"report_path={payload['smoothed_forward_progress_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-forward-progress")
def dynamic_v3_validate_smoothed_forward_progress_command(
    progress_id: Annotated[str, typer.Option("--progress-id", help="progress id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed forward progress artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
) -> None:
    """校验 TRADING-266 smoothed forward progress artifact。"""
    payload = system_target.validate_smoothed_forward_progress_artifact(
        progress_id=progress_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
@dynamic_v3_smoothed_weekly_dashboard_app.command("build")
def dynamic_v3_smoothed_weekly_dashboard_build_command(
    progress_id: Annotated[str, typer.Option("--progress-id", help="progress id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed weekly dashboard artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
) -> None:
    """生成 TRADING-267 smoothed weekly evidence dashboard。"""
    result = system_target.build_smoothed_weekly_dashboard(
        progress_id=progress_id,
        output_dir=output_dir,
    )
    summary = result["smoothed_dashboard_summary"]
    typer.echo(f"dashboard_id={result['dashboard_id']}")
    typer.echo(f"dashboard_dir={result['dashboard_dir']}")
    typer.echo(f"forward_confirmation_status={summary['forward_confirmation_status']}")
    typer.echo(f"ready_for_switch_recheck={summary['ready_for_switch_recheck']}")
    typer.echo(f"weekly_recommendation={summary['weekly_recommendation']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_weekly_dashboard_app.command("report")
def dynamic_v3_smoothed_weekly_dashboard_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest smoothed dashboard。"),
    ] = False,
    dashboard_id: Annotated[
        str | None,
        typer.Option("--dashboard-id", help="dashboard id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed weekly dashboard artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
) -> None:
    """展示 TRADING-267 smoothed weekly dashboard 摘要。"""
    payload = system_target.smoothed_weekly_dashboard_report_payload(
        dashboard_id=dashboard_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("smoothed_dashboard_summary"))
    typer.echo(f"dashboard_id={payload['dashboard_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"forward_confirmation_status={summary.get('forward_confirmation_status')}")
    typer.echo(f"ready_for_switch_recheck={summary.get('ready_for_switch_recheck')}")
    typer.echo(f"weekly_recommendation={summary.get('weekly_recommendation')}")
    typer.echo(f"report_path={payload['smoothed_weekly_dashboard_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-weekly-dashboard")
def dynamic_v3_validate_smoothed_weekly_dashboard_command(
    dashboard_id: Annotated[str, typer.Option("--dashboard-id", help="dashboard id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed weekly dashboard artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
) -> None:
    """校验 TRADING-267 smoothed weekly dashboard artifact。"""
    payload = system_target.validate_smoothed_weekly_dashboard_artifact(
        dashboard_id=dashboard_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
@dynamic_v3_smoothed_event_monitor_app.command("update")
def dynamic_v3_smoothed_event_monitor_update_command(
    progress_id: Annotated[str, typer.Option("--progress-id", help="progress id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed event monitor artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
) -> None:
    """运行 TRADING-268 sideways/recovery event accumulation monitor。"""
    result = system_target.update_smoothed_event_monitor(
        progress_id=progress_id,
        output_dir=output_dir,
    )
    summary = result["event_accumulation_summary"]
    sideways = _mapping_obj(summary.get("sideways_events"))
    recovery = _mapping_obj(summary.get("recovery_events"))
    typer.echo(f"monitor_id={result['monitor_id']}")
    typer.echo(f"monitor_dir={result['monitor_dir']}")
    typer.echo(f"sideways_events={sideways.get('available')}/{sideways.get('required')}")
    typer.echo(f"recovery_events={recovery.get('available')}/{recovery.get('required')}")
    typer.echo(f"recovery_lag_status={summary.get('recovery_lag_status')}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_event_monitor_app.command("report")
def dynamic_v3_smoothed_event_monitor_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest event monitor。"),
    ] = False,
    monitor_id: Annotated[str | None, typer.Option("--monitor-id", help="monitor id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed event monitor artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
) -> None:
    """展示 TRADING-268 smoothed event monitor 摘要。"""
    payload = system_target.smoothed_event_monitor_report_payload(
        monitor_id=monitor_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("event_accumulation_summary"))
    sideways = _mapping_obj(summary.get("sideways_events"))
    recovery = _mapping_obj(summary.get("recovery_events"))
    typer.echo(f"monitor_id={payload['monitor_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"sideways_events={sideways.get('available')}/{sideways.get('required')}")
    typer.echo(f"recovery_events={recovery.get('available')}/{recovery.get('required')}")
    typer.echo(f"lag_warning_count={summary.get('lag_warning_count')}")
    typer.echo(f"report_path={payload['smoothed_event_monitor_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-event-monitor")
def dynamic_v3_validate_smoothed_event_monitor_command(
    monitor_id: Annotated[str, typer.Option("--monitor-id", help="monitor id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed event monitor artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
) -> None:
    """校验 TRADING-268 smoothed event monitor artifact。"""
    payload = system_target.validate_smoothed_event_monitor_artifact(
        monitor_id=monitor_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_switch_readiness_app.command("recheck")
def dynamic_v3_smoothed_switch_readiness_recheck_command(
    dashboard_id: Annotated[str, typer.Option("--dashboard-id", help="dashboard id。")],
    monitor_id: Annotated[str, typer.Option("--monitor-id", help="monitor id。")],
    switch_plan_id: Annotated[
        str,
        typer.Option("--switch-plan-id", help="paper shadow switch plan id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed switch readiness artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
) -> None:
    """运行 TRADING-269 primary candidate switch readiness recheck。"""
    result = system_target.recheck_smoothed_switch_readiness(
        dashboard_id=dashboard_id,
        monitor_id=monitor_id,
        switch_plan_id=switch_plan_id,
        output_dir=output_dir,
    )
    decision = result["switch_readiness_decision"]
    typer.echo(f"recheck_id={result['recheck_id']}")
    typer.echo(f"recheck_dir={result['recheck_dir']}")
    typer.echo(f"recheck_decision={decision['recheck_decision']}")
    typer.echo(f"decision_confidence={decision['decision_confidence']}")
    typer.echo(f"can_execute_switch={decision['can_execute_switch']}")
    typer.echo(f"owner_decision_required={decision['owner_decision_required']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_switch_readiness_app.command("report")
def dynamic_v3_smoothed_switch_readiness_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest switch readiness。"),
    ] = False,
    recheck_id: Annotated[str | None, typer.Option("--recheck-id", help="recheck id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed switch readiness artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
) -> None:
    """展示 TRADING-269 switch readiness recheck 摘要。"""
    payload = system_target.smoothed_switch_readiness_report_payload(
        recheck_id=recheck_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("switch_readiness_decision"))
    criteria = _mapping_obj(payload.get("switch_readiness_criteria"))
    not_met = [
        row.get("criterion")
        for row in _records_obj(criteria.get("criteria"))
        if row.get("status") != "PASS"
    ]
    typer.echo(f"recheck_id={payload['recheck_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recheck_decision={decision.get('recheck_decision')}")
    typer.echo(f"criteria_not_met={','.join(str(item) for item in not_met)}")
    typer.echo(f"can_execute_switch={decision.get('can_execute_switch')}")
    typer.echo(f"report_path={payload['smoothed_switch_readiness_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-switch-readiness")
def dynamic_v3_validate_smoothed_switch_readiness_command(
    recheck_id: Annotated[str, typer.Option("--recheck-id", help="recheck id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed switch readiness artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
) -> None:
    """校验 TRADING-269 smoothed switch readiness artifact。"""
    payload = system_target.validate_smoothed_switch_readiness_artifact(
        recheck_id=recheck_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("can_execute_switch=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_owner_renewal_app.command("pack")
def dynamic_v3_smoothed_owner_renewal_pack_command(
    recheck_id: Annotated[str, typer.Option("--recheck-id", help="recheck id。")],
    owner_promotion_id: Annotated[
        str,
        typer.Option("--owner-promotion-id", help="owner promotion id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed owner renewal artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
) -> None:
    """生成 TRADING-270 smoothed owner decision renewal pack。"""
    result = system_target.build_smoothed_owner_renewal_pack(
        recheck_id=recheck_id,
        owner_promotion_id=owner_promotion_id,
        output_dir=output_dir,
    )
    options = result["owner_renewal_options"]
    typer.echo(f"renewal_id={result['renewal_id']}")
    typer.echo(f"renewal_dir={result['renewal_dir']}")
    typer.echo(f"previous_owner_decision={options['previous_owner_decision']}")
    typer.echo(f"current_recheck_decision={options['current_recheck_decision']}")
    typer.echo(f"recommended_owner_action={options['recommended_owner_action']}")
    typer.echo("auto_switch=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_owner_renewal_app.command("report")
def dynamic_v3_smoothed_owner_renewal_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner renewal。"),
    ] = False,
    renewal_id: Annotated[str | None, typer.Option("--renewal-id", help="renewal id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed owner renewal artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
) -> None:
    """展示 TRADING-270 owner renewal pack 摘要。"""
    payload = system_target.smoothed_owner_renewal_report_payload(
        renewal_id=renewal_id,
        latest=latest,
        output_dir=output_dir,
    )
    options = _mapping_obj(payload.get("owner_renewal_options"))
    typer.echo(f"renewal_id={payload['renewal_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"previous_owner_decision={options.get('previous_owner_decision')}")
    typer.echo(f"current_recheck_decision={options.get('current_recheck_decision')}")
    typer.echo(f"recommended_owner_action={options.get('recommended_owner_action')}")
    typer.echo(f"report_path={payload['smoothed_owner_renewal_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-owner-renewal")
def dynamic_v3_validate_smoothed_owner_renewal_command(
    renewal_id: Annotated[str, typer.Option("--renewal-id", help="renewal id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed owner renewal artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
) -> None:
    """校验 TRADING-270 smoothed owner renewal artifact。"""
    payload = system_target.validate_smoothed_owner_renewal_artifact(
        renewal_id=renewal_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_switch=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
