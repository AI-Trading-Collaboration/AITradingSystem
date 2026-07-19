# ruff: noqa: E501

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_signal_input_completeness as completeness
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_input_completeness_recovery as completeness_recovery,
)
from ai_trading_system.etf_portfolio import dynamic_v3_signal_input_recovery as recovery
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_signal_input_completeness_app,
    dynamic_v3_signal_input_completeness_recovery_app,
    dynamic_v3_signal_input_recovery_app,
)


def _as_of(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("--as-of must be YYYY-MM-DD") from exc


def _echo_validation(payload: dict[str, object]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_signal_input_completeness_app.command("run")
def dynamic_v3_signal_input_completeness_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="signal input completeness as-of date YYYY-MM-DD；省略时使用当前 UTC 日期。",
        ),
    ] = None,
    policy_path: Annotated[
        Path,
        typer.Option("--policy-path", help="signal input completeness policy YAML。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input completeness artifact root。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
) -> None:
    result = completeness.run_signal_input_completeness_monitor(
        as_of=_as_of(as_of), policy_path=policy_path, output_dir=output_dir
    )
    report = _mapping(result.get("signal_input_completeness_report"))
    validation = _mapping(result.get("signal_input_completeness_validation"))
    typer.echo(f"monitor_id={result['monitor_id']}")
    typer.echo(f"signal_input_status={report.get('signal_input_status')}")
    typer.echo(f"blocking_count={report.get('blocking_count')}")
    typer.echo(f"warning_count={report.get('warning_count')}")
    typer.echo(f"validation_status={validation.get('status')}")
    typer.echo("production_effect=none")


@dynamic_v3_signal_input_completeness_app.command("report")
def dynamic_v3_signal_input_completeness_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    monitor_id: Annotated[
        str | None,
        typer.Option("--monitor-id", help="signal input completeness monitor id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input completeness artifact root。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
) -> None:
    if not latest and not monitor_id:
        raise typer.BadParameter("--monitor-id or --latest is required")
    payload = completeness.signal_input_completeness_report_payload(
        monitor_id=monitor_id, latest=latest, output_dir=output_dir
    )
    report = _mapping(payload.get("signal_input_completeness_report"))
    typer.echo(f"monitor_id={payload['monitor_id']}")
    typer.echo(f"signal_input_status={report.get('signal_input_status')}")
    typer.echo(f"report_path={payload['signal_input_completeness_markdown_path']}")


@dynamic_v3_rescue_app.command("validate-signal-input-completeness")
def dynamic_v3_validate_signal_input_completeness_command(
    monitor_id: Annotated[
        str | None,
        typer.Option("--monitor-id", help="signal input completeness monitor id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    policy_path: Annotated[
        Path,
        typer.Option("--policy-path", help="signal input completeness policy YAML。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input completeness artifact root。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
) -> None:
    resolved = monitor_id
    if latest:
        resolved = str(completeness.signal_input_completeness_report_payload(
            latest=True, output_dir=output_dir
        ).get("monitor_id") or "")
    if not resolved:
        raise typer.BadParameter("--monitor-id or --latest is required")
    _echo_validation(completeness.validate_signal_input_completeness_artifact(
        monitor_id=resolved, output_dir=output_dir, policy_path=policy_path
    ))


@dynamic_v3_signal_input_completeness_recovery_app.command("run")
def dynamic_v3_signal_input_completeness_recovery_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="signal input completeness recovery as-of date YYYY-MM-DD。",
        ),
    ] = None,
    monitor_id: Annotated[
        str | None,
        typer.Option("--monitor-id", help="已有 signal input completeness monitor id。"),
    ] = None,
    rerun_monitor: Annotated[
        bool,
        typer.Option(
            "--rerun-monitor/--use-existing-monitor",
            help="是否先重新运行 signal input completeness monitor。",
        ),
    ] = True,
    policy_path: Annotated[
        Path,
        typer.Option("--policy-path", help="signal input completeness policy YAML。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    signal_input_dir: Annotated[
        Path,
        typer.Option("--signal-input-dir", help="signal input completeness artifact root。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input completeness recovery artifact root。"),
    ] = completeness_recovery.DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
) -> None:
    result = completeness_recovery.run_signal_input_completeness_recovery(
        as_of=_as_of(as_of), monitor_id=monitor_id, rerun_monitor=rerun_monitor,
        policy_path=policy_path, signal_input_dir=signal_input_dir, output_dir=output_dir,
    )
    report = _mapping(result.get("signal_input_completeness_recovery_report"))
    validation = _mapping(result.get("signal_input_completeness_recovery_validation"))
    typer.echo(f"recovery_id={result['recovery_id']}")
    typer.echo(f"recovery_status={report.get('recovery_status')}")
    typer.echo(f"prior_monitor_id={report.get('prior_monitor_id')}")
    typer.echo(f"restored_monitor_id={report.get('restored_monitor_id')}")
    typer.echo(f"validation_status={validation.get('status')}")
    typer.echo("production_effect=none")


@dynamic_v3_signal_input_completeness_recovery_app.command("report")
def dynamic_v3_signal_input_completeness_recovery_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    recovery_id: Annotated[
        str | None,
        typer.Option("--recovery-id", help="signal input completeness recovery id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input completeness recovery artifact root。"),
    ] = completeness_recovery.DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
) -> None:
    if not latest and not recovery_id:
        raise typer.BadParameter("--recovery-id or --latest is required")
    payload = completeness_recovery.signal_input_completeness_recovery_report_payload(
        recovery_id=recovery_id, latest=latest, output_dir=output_dir
    )
    report = _mapping(payload.get("signal_input_completeness_recovery_report"))
    typer.echo(f"recovery_id={payload['recovery_id']}")
    typer.echo(f"recovery_status={report.get('recovery_status')}")
    typer.echo(f"report_path={payload['signal_input_completeness_recovery_markdown_path']}")


@dynamic_v3_rescue_app.command("validate-signal-input-completeness-recovery")
def dynamic_v3_validate_signal_input_completeness_recovery_command(
    recovery_id: Annotated[
        str | None,
        typer.Option("--recovery-id", help="signal input completeness recovery id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input completeness recovery artifact root。"),
    ] = completeness_recovery.DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
) -> None:
    resolved = recovery_id
    if latest:
        resolved = str(completeness_recovery.signal_input_completeness_recovery_report_payload(
            latest=True, output_dir=output_dir
        ).get("recovery_id") or "")
    if not resolved:
        raise typer.BadParameter("--recovery-id or --latest is required")
    _echo_validation(completeness_recovery.validate_signal_input_completeness_recovery_artifact(
        recovery_id=resolved, output_dir=output_dir
    ))


@dynamic_v3_signal_input_recovery_app.command("run")
def dynamic_v3_signal_input_recovery_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="signal input recovery as-of date YYYY-MM-DD；省略时读取 monitor as_of。",
        ),
    ] = None,
    restored_monitor_id: Annotated[
        str | None,
        typer.Option("--restored-monitor-id", help="恢复后 signal input completeness monitor id。"),
    ] = None,
    previous_monitor_id: Annotated[
        str | None,
        typer.Option("--previous-monitor-id", help="恢复前 blocking monitor id。"),
    ] = None,
    signal_input_dir: Annotated[
        Path,
        typer.Option("--signal-input-dir", help="signal input completeness artifact root。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    policy_path: Annotated[
        Path,
        typer.Option("--policy-path", help="signal input completeness policy YAML。"),
    ] = completeness.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input recovery artifact root。"),
    ] = recovery.DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
) -> None:
    result = recovery.run_signal_input_root_cause_recovery(
        as_of=_as_of(as_of), restored_monitor_id=restored_monitor_id,
        previous_monitor_id=previous_monitor_id, signal_input_dir=signal_input_dir,
        policy_path=policy_path, output_dir=output_dir,
    )
    report = _mapping(result.get("signal_input_recovery_report"))
    validation = _mapping(result.get("signal_input_recovery_validation"))
    typer.echo(f"recovery_id={result['recovery_id']}")
    typer.echo(f"restoration_status={report.get('restoration_status')}")
    typer.echo(f"previous_monitor_id={report.get('previous_monitor_id')}")
    typer.echo(f"restored_monitor_id={report.get('restored_monitor_id')}")
    typer.echo(f"validation_status={validation.get('status')}")
    typer.echo("production_effect=none")


@dynamic_v3_signal_input_recovery_app.command("report")
def dynamic_v3_signal_input_recovery_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    recovery_id: Annotated[
        str | None,
        typer.Option("--recovery-id", help="signal input recovery id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input recovery artifact root。"),
    ] = recovery.DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
) -> None:
    if not latest and not recovery_id:
        raise typer.BadParameter("--recovery-id or --latest is required")
    payload = recovery.signal_input_recovery_report_payload(
        recovery_id=recovery_id, latest=latest, output_dir=output_dir
    )
    report = _mapping(payload.get("signal_input_recovery_report"))
    typer.echo(f"recovery_id={payload['recovery_id']}")
    typer.echo(f"restoration_status={report.get('restoration_status')}")
    typer.echo(f"report_path={payload['signal_input_recovery_markdown_path']}")


@dynamic_v3_rescue_app.command("validate-signal-input-recovery")
def dynamic_v3_validate_signal_input_recovery_command(
    recovery_id: Annotated[
        str | None,
        typer.Option("--recovery-id", help="signal input recovery id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="signal input recovery artifact root。"),
    ] = recovery.DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
) -> None:
    resolved = recovery_id
    if latest:
        resolved = str(recovery.signal_input_recovery_report_payload(
            latest=True, output_dir=output_dir
        ).get("recovery_id") or "")
    if not resolved:
        raise typer.BadParameter("--recovery-id or --latest is required")
    _echo_validation(recovery.validate_signal_input_recovery_artifact(
        recovery_id=resolved, output_dir=output_dir
    ))
