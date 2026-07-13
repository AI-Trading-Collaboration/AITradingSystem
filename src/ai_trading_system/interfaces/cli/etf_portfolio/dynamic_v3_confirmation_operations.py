from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR,
    DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    DEFAULT_CONFIRMATION_DASHBOARD_DIR,
    DEFAULT_PRESSURE_REGIME_TAG_DIR,
    DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH,
    DEFAULT_RULE_REVIEW_QUEUE_DIR,
    build_confirmation_cycle_plan,
    build_confirmation_dashboard,
    build_rule_review_queue,
    confirmation_cycle_weekly_report_payload,
    confirmation_dashboard_report_payload,
    pressure_regime_tag_report_payload,
    rule_review_queue_report_payload,
    run_confirmation_cycle_weekly,
    run_pressure_regime_tagging,
    validate_confirmation_cycle_schedule_config,
    validate_confirmation_cycle_weekly_artifact,
    validate_confirmation_dashboard_artifact,
    validate_pressure_regime_tag_artifact,
    validate_pressure_regime_tagging_config,
    validate_rule_review_queue_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_confirmation_cycle_app,
    dynamic_v3_confirmation_dashboard_app,
    dynamic_v3_pressure_regime_tag_app,
    dynamic_v3_rescue_app,
    dynamic_v3_rule_review_queue_app,
)


def _parse_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD") from exc


def _echo_validation(payload: Mapping[str, Any]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_confirmation_cycle_app.command("plan")
def dynamic_v3_confirmation_cycle_plan_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="confirmation cycle schedule config。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation cycle plan artifact root。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR,
) -> None:
    """生成 TRADING-179 weekly confirmation cycle command pack。"""
    result = build_confirmation_cycle_plan(config_path=config_path, output_dir=output_dir)
    manifest = result["manifest"]
    pack = result["scheduled_command_pack"]
    typer.echo(f"plan_id={result['plan_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"planned_step_count={manifest['planned_step_count']}")
    typer.echo(f"scheduled_command_count={len(pack['commands'])}")
    typer.echo(f"report_path={manifest['confirmation_cycle_plan_report_path']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_confirmation_cycle_app.command("runbook")
def dynamic_v3_confirmation_cycle_runbook_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="confirmation cycle schedule config。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation cycle plan artifact root。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR,
) -> None:
    """生成 TRADING-179 weekly confirmation cycle runbook。"""
    result = build_confirmation_cycle_plan(config_path=config_path, output_dir=output_dir)
    manifest = result["manifest"]
    typer.echo(f"plan_id={result['plan_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"runbook_path={manifest['confirmation_cycle_runbook_path']}")
    typer.echo("auto_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_confirmation_cycle_app.command("validate-config")
def dynamic_v3_confirmation_cycle_validate_config_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="confirmation cycle schedule config。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
) -> None:
    """校验 TRADING-179 confirmation cycle schedule config。"""
    _echo_validation(validate_confirmation_cycle_schedule_config(config_path=config_path))


@dynamic_v3_confirmation_cycle_app.command("weekly-run")
def dynamic_v3_confirmation_cycle_weekly_run_command(
    week_ending: Annotated[
        str,
        typer.Option("--week-ending", help="week ending date YYYY-MM-DD。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="confirmation cycle schedule config。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    execute_ready_updates: Annotated[
        bool,
        typer.Option(
            "--execute-ready-updates/--dry-run",
            help="显式允许执行 READY_TO_UPDATE outcome update；默认 dry-run。",
        ),
    ] = False,
    registry_id: Annotated[
        str | None,
        typer.Option("--registry-id", "--registry_id", help="可选 confirmation registry id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly cycle artifact root。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
) -> None:
    """运行 TRADING-180 weekly confirmation cycle。"""
    result = run_confirmation_cycle_weekly(
        week_ending=_parse_date(week_ending, "--week-ending"),
        config_path=config_path,
        execute_ready_updates=execute_ready_updates,
        registry_id=registry_id,
        output_dir=output_dir,
    )
    summary = result["weekly_cycle_summary"]
    typer.echo(f"weekly_cycle_id={result['weekly_cycle_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"dry_run={result['manifest']['dry_run']}")
    typer.echo(f"due_windows={summary['due_windows']}")
    typer.echo(f"updated_windows={summary['updated_windows']}")
    typer.echo(f"ready_for_evaluation={summary['ready_for_evaluation']}")
    typer.echo(f"rule_review_recommendation={summary['rule_review_recommendation']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_confirmation_cycle_app.command("weekly-report")
def dynamic_v3_confirmation_cycle_weekly_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest weekly cycle。"),
    ] = False,
    weekly_cycle_id: Annotated[
        str | None,
        typer.Option("--weekly-cycle-id", "--weekly_cycle_id", help="weekly cycle id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly cycle artifact root。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
) -> None:
    """展示 TRADING-180 weekly confirmation cycle 摘要。"""
    payload = confirmation_cycle_weekly_report_payload(
        weekly_cycle_id=weekly_cycle_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["weekly_cycle_summary"]
    typer.echo(f"weekly_cycle_id={payload['weekly_cycle_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"due_windows={summary['due_windows']}")
    typer.echo(f"updated_windows={summary['updated_windows']}")
    typer.echo(f"ready_for_evaluation={summary['ready_for_evaluation']}")
    typer.echo(f"report_path={payload['weekly_cycle_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-confirmation-cycle-weekly")
def dynamic_v3_validate_confirmation_cycle_weekly_command(
    weekly_cycle_id: Annotated[
        str,
        typer.Option("--weekly-cycle-id", "--weekly_cycle_id", help="weekly cycle id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly cycle artifact root。"),
    ] = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
) -> None:
    """校验 TRADING-180 weekly confirmation cycle artifact。"""
    _echo_validation(
        validate_confirmation_cycle_weekly_artifact(
            weekly_cycle_id=weekly_cycle_id, output_dir=output_dir
        )
    )


@dynamic_v3_pressure_regime_tag_app.command("run")
def dynamic_v3_pressure_regime_tag_run_command(
    start: Annotated[str, typer.Option("--start", help="start date YYYY-MM-DD。")],
    end: Annotated[str, typer.Option("--end", help="end date YYYY-MM-DD。")],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="pressure regime tagging config。"),
    ] = DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure regime tag artifact root。"),
    ] = DEFAULT_PRESSURE_REGIME_TAG_DIR,
) -> None:
    """运行 TRADING-181 pressure regime outcome tagging。"""
    result = run_pressure_regime_tagging(
        start=_parse_date(start, "--start"),
        end=_parse_date(end, "--end"),
        config_path=config_path,
        output_dir=output_dir,
    )
    summary = result["pressure_regime_summary"]
    samples = summary["pressure_samples"]
    typer.echo(f"tag_id={result['tag_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"tech_drawdown_count={samples['tech_drawdown']}")
    typer.echo(f"risk_off_count={samples['risk_off']}")
    typer.echo(f"semiconductor_pullback_count={samples['semiconductor_pullback']}")
    typer.echo(
        "defensive_validation_relevant_outcomes="
        f"{summary['defensive_validation_relevant_outcomes']}"
    )
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_pressure_regime_tag_app.command("validate-config")
def dynamic_v3_pressure_regime_tag_validate_config_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="pressure regime tagging config。"),
    ] = DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH,
) -> None:
    """校验 TRADING-181 pressure regime tagging 配置。"""
    _echo_validation(validate_pressure_regime_tagging_config(config_path=config_path))


@dynamic_v3_pressure_regime_tag_app.command("report")
def dynamic_v3_pressure_regime_tag_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest pressure regime tag。"),
    ] = False,
    tag_id: Annotated[
        str | None,
        typer.Option("--tag-id", "--tag_id", help="pressure regime tag id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure regime tag artifact root。"),
    ] = DEFAULT_PRESSURE_REGIME_TAG_DIR,
) -> None:
    """展示 TRADING-181 pressure regime tagging 摘要。"""
    payload = pressure_regime_tag_report_payload(
        tag_id=tag_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["pressure_regime_summary"]
    samples = summary["pressure_samples"]
    typer.echo(f"tag_id={payload['tag_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"tech_drawdown_count={samples['tech_drawdown']}")
    typer.echo(f"risk_off_count={samples['risk_off']}")
    typer.echo(f"semiconductor_pullback_count={samples['semiconductor_pullback']}")
    typer.echo(f"report_path={payload['pressure_regime_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-pressure-regime-tag")
def dynamic_v3_validate_pressure_regime_tag_command(
    tag_id: Annotated[str, typer.Option("--tag-id", "--tag_id", help="pressure tag id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure regime tag artifact root。"),
    ] = DEFAULT_PRESSURE_REGIME_TAG_DIR,
) -> None:
    """校验 TRADING-181 pressure regime tag artifact。"""
    _echo_validation(validate_pressure_regime_tag_artifact(tag_id=tag_id, output_dir=output_dir))


@dynamic_v3_confirmation_dashboard_app.command("build")
def dynamic_v3_confirmation_dashboard_build_command(
    week_ending: Annotated[
        str,
        typer.Option("--week-ending", help="week ending date YYYY-MM-DD。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation dashboard artifact root。"),
    ] = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
) -> None:
    """生成 TRADING-182 confirmation evidence dashboard。"""
    result = build_confirmation_dashboard(
        week_ending=_parse_date(week_ending, "--week-ending"), output_dir=output_dir
    )
    summary = result["confirmation_dashboard_summary"]
    typer.echo(f"dashboard_id={result['dashboard_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"targets_total={summary['targets_total']}")
    typer.echo(f"ready_for_evaluation={summary['ready_for_evaluation']}")
    typer.echo(f"dashboard_recommendation={summary['dashboard_recommendation']}")
    typer.echo("policy_change_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_confirmation_dashboard_app.command("report")
def dynamic_v3_confirmation_dashboard_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest confirmation dashboard。"),
    ] = False,
    dashboard_id: Annotated[
        str | None,
        typer.Option("--dashboard-id", "--dashboard_id", help="dashboard id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation dashboard artifact root。"),
    ] = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
) -> None:
    """展示 TRADING-182 confirmation evidence dashboard 摘要。"""
    payload = confirmation_dashboard_report_payload(
        dashboard_id=dashboard_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["confirmation_dashboard_summary"]
    typer.echo(f"dashboard_id={payload['dashboard_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"targets_total={summary['targets_total']}")
    typer.echo(f"ready_for_evaluation={summary['ready_for_evaluation']}")
    typer.echo(f"report_path={payload['confirmation_dashboard_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-confirmation-dashboard")
def dynamic_v3_validate_confirmation_dashboard_command(
    dashboard_id: Annotated[
        str,
        typer.Option("--dashboard-id", "--dashboard_id", help="dashboard id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="confirmation dashboard artifact root。"),
    ] = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
) -> None:
    """校验 TRADING-182 confirmation dashboard artifact。"""
    _echo_validation(
        validate_confirmation_dashboard_artifact(dashboard_id=dashboard_id, output_dir=output_dir)
    )


@dynamic_v3_rule_review_queue_app.command("build")
def dynamic_v3_rule_review_queue_build_command(
    cycle_id: Annotated[
        str | None,
        typer.Option("--cycle-id", "--cycle_id", help="可选 source rule review cycle id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule review queue artifact root。"),
    ] = DEFAULT_RULE_REVIEW_QUEUE_DIR,
) -> None:
    """生成 TRADING-183 owner rule review queue。"""
    result = build_rule_review_queue(cycle_id=cycle_id, output_dir=output_dir)
    summary = result["queue_summary"]
    typer.echo(f"queue_id={result['queue_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"pending_count={summary['pending_count']}")
    typer.echo(f"ready_for_owner_review_count={summary['ready_for_owner_review_count']}")
    typer.echo(f"not_ready_count={summary['not_ready_count']}")
    typer.echo("policy_change_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rule_review_queue_app.command("report")
def dynamic_v3_rule_review_queue_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest rule review queue。"),
    ] = False,
    queue_id: Annotated[
        str | None,
        typer.Option("--queue-id", "--queue_id", help="rule review queue id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule review queue artifact root。"),
    ] = DEFAULT_RULE_REVIEW_QUEUE_DIR,
) -> None:
    """展示 TRADING-183 owner rule review queue 摘要。"""
    payload = rule_review_queue_report_payload(
        queue_id=queue_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["queue_summary"]
    typer.echo(f"queue_id={payload['queue_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"pending_count={summary['pending_count']}")
    typer.echo(f"ready_for_owner_review_count={summary['ready_for_owner_review_count']}")
    typer.echo(f"not_ready_count={summary['not_ready_count']}")
    typer.echo(f"report_path={payload['rule_review_queue_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-rule-review-queue")
def dynamic_v3_validate_rule_review_queue_command(
    queue_id: Annotated[str, typer.Option("--queue-id", "--queue_id", help="queue id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rule review queue artifact root。"),
    ] = DEFAULT_RULE_REVIEW_QUEUE_DIR,
) -> None:
    """校验 TRADING-183 rule review queue artifact。"""
    _echo_validation(validate_rule_review_queue_artifact(queue_id=queue_id, output_dir=output_dir))


__all__ = [
    "dynamic_v3_confirmation_cycle_plan_command",
    "dynamic_v3_confirmation_cycle_runbook_command",
    "dynamic_v3_confirmation_cycle_validate_config_command",
    "dynamic_v3_confirmation_cycle_weekly_report_command",
    "dynamic_v3_confirmation_cycle_weekly_run_command",
    "dynamic_v3_confirmation_dashboard_build_command",
    "dynamic_v3_confirmation_dashboard_report_command",
    "dynamic_v3_pressure_regime_tag_report_command",
    "dynamic_v3_pressure_regime_tag_run_command",
    "dynamic_v3_pressure_regime_tag_validate_config_command",
    "dynamic_v3_rule_review_queue_build_command",
    "dynamic_v3_rule_review_queue_report_command",
    "dynamic_v3_validate_confirmation_cycle_weekly_command",
    "dynamic_v3_validate_confirmation_dashboard_command",
    "dynamic_v3_validate_pressure_regime_tag_command",
    "dynamic_v3_validate_rule_review_queue_command",
]
