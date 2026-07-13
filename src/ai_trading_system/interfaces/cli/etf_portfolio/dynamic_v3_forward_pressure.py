from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_forward_pressure as forward_pressure
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_forward_pressure_capture_app,
    dynamic_v3_pressure_capture_app,
    dynamic_v3_pressure_sample_ledger_app,
    dynamic_v3_pressure_trigger_app,
    dynamic_v3_rescue_app,
    dynamic_v3_weekly_defensive_evidence_app,
)


def _parse_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD") from exc


def _echo_validation_payload(payload: Mapping[str, Any]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_forward_pressure_capture_app.command("plan")
def dynamic_v3_forward_pressure_capture_plan_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="forward pressure capture config。"),
    ] = forward_pressure.DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward pressure capture artifact root。"),
    ] = forward_pressure.DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
) -> None:
    """运行 TRADING-194 forward pressure capture plan。"""
    result = forward_pressure.build_forward_pressure_capture_plan(
        config_path=config_path,
        output_dir=output_dir,
    )
    typer.echo(f"capture_plan_id={result['capture_plan_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"daily_commands={len(result['daily_command_pack']['commands'])}")
    typer.echo(f"weekly_commands={len(result['weekly_command_pack']['commands'])}")
    typer.echo(f"event_commands={len(result['event_driven_trigger_plan']['commands'])}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_forward_pressure_capture_app.command("report")
def dynamic_v3_forward_pressure_capture_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest forward pressure plan。"),
    ] = False,
    capture_plan_id: Annotated[
        str | None,
        typer.Option("--capture-plan-id", "--capture_plan_id", help="capture plan id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward pressure capture artifact root。"),
    ] = forward_pressure.DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
) -> None:
    """展示 TRADING-194 forward pressure capture plan 摘要。"""
    payload = forward_pressure.forward_pressure_capture_report_payload(
        capture_plan_id=capture_plan_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"capture_plan_id={payload['capture_plan_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['forward_pressure_capture_report_path']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-forward-pressure-capture")
def dynamic_v3_validate_forward_pressure_capture_command(
    capture_plan_id: Annotated[
        str,
        typer.Option("--capture-plan-id", "--capture_plan_id", help="capture plan id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="forward pressure capture artifact root。"),
    ] = forward_pressure.DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
) -> None:
    """校验 TRADING-194 forward pressure capture artifact。"""
    _echo_validation_payload(
        forward_pressure.validate_forward_pressure_capture_artifact(
            capture_plan_id=capture_plan_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_pressure_trigger_app.command("scan")
def dynamic_v3_pressure_trigger_scan_command(
    as_of: Annotated[str, typer.Option("--as-of", "--date", help="as-of date YYYY-MM-DD。")],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="forward pressure capture config。"),
    ] = forward_pressure.DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure trigger artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_TRIGGER_DIR,
    enforce_data_quality_gate: Annotated[
        bool,
        typer.Option(
            "--enforce-data-quality-gate/--skip-data-quality-gate",
            help="是否运行 cached data quality gate。",
        ),
    ] = True,
) -> None:
    """运行 TRADING-195 daily pressure trigger scanner。"""
    result = forward_pressure.run_pressure_trigger_scan(
        as_of=_parse_date(as_of, "--as-of"),
        config_path=config_path,
        output_dir=output_dir,
        enforce_data_quality_gate=enforce_data_quality_gate,
    )
    metrics = result["trigger_metrics"]
    actions = result["triggered_actions"]
    typer.echo(f"trigger_id={result['trigger_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"trigger_status={metrics['trigger_status']}")
    typer.echo(f"event_driven_capture_required={actions['event_driven_capture_required']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_pressure_trigger_app.command("report")
def dynamic_v3_pressure_trigger_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest pressure trigger。"),
    ] = False,
    trigger_id: Annotated[
        str | None,
        typer.Option("--trigger-id", "--trigger_id", help="pressure trigger id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure trigger artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_TRIGGER_DIR,
) -> None:
    """展示 TRADING-195 pressure trigger 摘要。"""
    payload = forward_pressure.pressure_trigger_report_payload(
        trigger_id=trigger_id,
        latest=latest,
        output_dir=output_dir,
    )
    metrics = payload["trigger_metrics"]
    actions = payload["triggered_actions"]
    typer.echo(f"trigger_id={payload['trigger_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"trigger_status={metrics['trigger_status']}")
    typer.echo(f"event_driven_capture_required={actions['event_driven_capture_required']}")
    typer.echo(f"report_path={payload['pressure_trigger_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-pressure-trigger")
def dynamic_v3_validate_pressure_trigger_command(
    trigger_id: Annotated[
        str,
        typer.Option("--trigger-id", "--trigger_id", help="pressure trigger id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure trigger artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_TRIGGER_DIR,
) -> None:
    """校验 TRADING-195 pressure trigger artifact。"""
    _echo_validation_payload(
        forward_pressure.validate_pressure_trigger_artifact(
            trigger_id=trigger_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_pressure_capture_app.command("run")
def dynamic_v3_pressure_capture_run_command(
    trigger_id: Annotated[
        str,
        typer.Option("--trigger-id", "--trigger_id", help="pressure trigger id。"),
    ],
    force: Annotated[
        bool,
        typer.Option("--force/--no-force", help="NO_TRIGGER 时是否手动 force workflow。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure capture artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_CAPTURE_DIR,
    enforce_data_quality_gate: Annotated[
        bool,
        typer.Option(
            "--enforce-data-quality-gate/--skip-data-quality-gate",
            help="是否运行 cached data quality gate。",
        ),
    ] = True,
) -> None:
    """运行 TRADING-196 event-driven pressure capture workflow。"""
    result = forward_pressure.run_pressure_capture_workflow(
        trigger_id=trigger_id,
        force=force,
        output_dir=output_dir,
        enforce_data_quality_gate=enforce_data_quality_gate,
    )
    steps = result["pressure_capture_steps"]
    typer.echo(f"capture_id={result['capture_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"trigger_status={steps['trigger_status']}")
    typer.echo(f"manual_force={steps['manual_force']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_pressure_capture_app.command("report")
def dynamic_v3_pressure_capture_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest pressure capture。"),
    ] = False,
    capture_id: Annotated[
        str | None,
        typer.Option("--capture-id", "--capture_id", help="pressure capture id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure capture artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_CAPTURE_DIR,
) -> None:
    """展示 TRADING-196 pressure capture 摘要。"""
    payload = forward_pressure.pressure_capture_report_payload(
        capture_id=capture_id,
        latest=latest,
        output_dir=output_dir,
    )
    steps = payload["pressure_capture_steps"]
    typer.echo(f"capture_id={payload['capture_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"trigger_status={steps['trigger_status']}")
    typer.echo(f"manual_force={steps['manual_force']}")
    typer.echo(f"report_path={payload['pressure_capture_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-pressure-capture")
def dynamic_v3_validate_pressure_capture_command(
    capture_id: Annotated[
        str,
        typer.Option("--capture-id", "--capture_id", help="pressure capture id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure capture artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_CAPTURE_DIR,
) -> None:
    """校验 TRADING-196 pressure capture artifact。"""
    _echo_validation_payload(
        forward_pressure.validate_pressure_capture_artifact(
            capture_id=capture_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_pressure_sample_ledger_app.command("update")
def dynamic_v3_pressure_sample_ledger_update_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="forward pressure capture config。"),
    ] = forward_pressure.DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure sample ledger artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
) -> None:
    """运行 TRADING-197 forward/PIT pressure sample ledger update。"""
    result = forward_pressure.update_pressure_sample_ledger(
        config_path=config_path,
        output_dir=output_dir,
    )
    summary = result["pressure_sample_summary"]
    typer.echo(f"ledger_id={result['ledger_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"forward_samples={summary['forward_samples']}")
    typer.echo(f"pit_replay_samples={summary['pit_replay_samples']}")
    typer.echo(f"simulation_samples={summary['simulation_samples']}")
    typer.echo(f"progress_to_requirement={summary['progress_to_requirement']}")
    typer.echo("production_effect=none")


@dynamic_v3_pressure_sample_ledger_app.command("report")
def dynamic_v3_pressure_sample_ledger_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest pressure sample ledger。"),
    ] = False,
    ledger_id: Annotated[
        str | None,
        typer.Option("--ledger-id", "--ledger_id", help="pressure sample ledger id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure sample ledger artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
) -> None:
    """展示 TRADING-197 pressure sample ledger 摘要。"""
    payload = forward_pressure.pressure_sample_ledger_report_payload(
        ledger_id=ledger_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["pressure_sample_summary"]
    typer.echo(f"ledger_id={payload['ledger_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"forward_samples={summary['forward_samples']}")
    typer.echo(f"pit_replay_samples={summary['pit_replay_samples']}")
    typer.echo(f"simulation_samples={summary['simulation_samples']}")
    typer.echo(f"report_path={payload['pressure_sample_ledger_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-pressure-sample-ledger")
def dynamic_v3_validate_pressure_sample_ledger_command(
    ledger_id: Annotated[
        str,
        typer.Option("--ledger-id", "--ledger_id", help="pressure sample ledger id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="pressure sample ledger artifact root。"),
    ] = forward_pressure.DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
) -> None:
    """校验 TRADING-197 pressure sample ledger artifact。"""
    _echo_validation_payload(
        forward_pressure.validate_pressure_sample_ledger_artifact(
            ledger_id=ledger_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_weekly_defensive_evidence_app.command("run")
def dynamic_v3_weekly_defensive_evidence_run_command(
    week_ending: Annotated[
        str,
        typer.Option("--week-ending", help="week ending date YYYY-MM-DD。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly defensive evidence artifact root。"),
    ] = forward_pressure.DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
) -> None:
    """运行 TRADING-198 weekly defensive evidence update。"""
    result = forward_pressure.run_weekly_defensive_evidence_update(
        week_ending=_parse_date(week_ending, "--week-ending"),
        output_dir=output_dir,
    )
    summary = result["weekly_defensive_summary"]
    typer.echo(f"weekly_defensive_id={result['weekly_defensive_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"new_forward_pressure_samples={summary['new_forward_pressure_samples']}")
    typer.echo(f"defensive_rule_status={summary['defensive_rule_status']}")
    typer.echo(f"weekly_recommendation={summary['weekly_recommendation']}")
    typer.echo("policy_change_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_weekly_defensive_evidence_app.command("report")
def dynamic_v3_weekly_defensive_evidence_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest weekly defensive evidence。"),
    ] = False,
    weekly_defensive_id: Annotated[
        str | None,
        typer.Option(
            "--weekly-defensive-id",
            "--weekly_defensive_id",
            help="weekly defensive id。",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly defensive evidence artifact root。"),
    ] = forward_pressure.DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
) -> None:
    """展示 TRADING-198 weekly defensive evidence 摘要。"""
    payload = forward_pressure.weekly_defensive_evidence_report_payload(
        weekly_defensive_id=weekly_defensive_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["weekly_defensive_summary"]
    typer.echo(f"weekly_defensive_id={payload['weekly_defensive_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"new_forward_pressure_samples={summary['new_forward_pressure_samples']}")
    typer.echo(f"defensive_rule_status={summary['defensive_rule_status']}")
    typer.echo(f"report_path={payload['weekly_defensive_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-weekly-defensive-evidence")
def dynamic_v3_validate_weekly_defensive_evidence_command(
    weekly_defensive_id: Annotated[
        str,
        typer.Option(
            "--weekly-defensive-id",
            "--weekly_defensive_id",
            help="weekly defensive id。",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weekly defensive evidence artifact root。"),
    ] = forward_pressure.DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
) -> None:
    """校验 TRADING-198 weekly defensive evidence artifact。"""
    _echo_validation_payload(
        forward_pressure.validate_weekly_defensive_evidence_artifact(
            weekly_defensive_id=weekly_defensive_id,
            output_dir=output_dir,
        )
    )
