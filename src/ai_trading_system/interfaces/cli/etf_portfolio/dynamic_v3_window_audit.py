from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_WINDOW_AUDIT_DIR,
    inspect_window_artifact,
    run_window_audit,
    validate_window_audit_artifact,
    window_audit_report_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj, parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_window_audit_app,
)


@dynamic_v3_window_audit_app.command("run")
def dynamic_v3_window_audit_run_command(
    as_of: Annotated[str, typer.Option("--as-of", help="requested window start date。")],
    end: Annotated[str, typer.Option("--end", help="requested window end date。")],
    artifact_root: Annotated[
        Path,
        typer.Option("--artifact-root", help="待扫描 artifact root。"),
    ] = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="window audit artifact root。"),
    ] = DEFAULT_WINDOW_AUDIT_DIR,
) -> None:
    """运行 TRADING-111 backtest window audit。"""
    result = run_window_audit(
        as_of=parse_date(as_of),
        end=parse_date(end),
        artifact_root=artifact_root,
        output_dir=output_dir,
    )
    report = result["report"]
    typer.echo(f"window_audit_id={result['window_audit_id']}")
    typer.echo(f"window_audit_dir={result['window_audit_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"configured_backtest_start={report['configured_backtest_start']}")
    typer.echo(f"earliest_actual_evaluation_start={report['earliest_actual_evaluation_start']}")
    typer.echo(f"promotion_blocking_count={report['promotion_blocking_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_window_audit_app.command("report")
def dynamic_v3_window_audit_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest window audit pointer。"),
    ] = False,
    audit_id: Annotated[str | None, typer.Option("--audit-id", help="window audit id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="window audit artifact root。"),
    ] = DEFAULT_WINDOW_AUDIT_DIR,
) -> None:
    """展示 TRADING-111 window audit 摘要。"""
    payload = window_audit_report_payload(
        audit_id=audit_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"window_audit_id={payload['window_audit_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"configured_backtest_start={payload['configured_backtest_start']}")
    typer.echo(f"earliest_actual_evaluation_start={payload['earliest_actual_evaluation_start']}")
    typer.echo(f"promotion_blocking_count={payload['promotion_blocking_count']}")
    typer.echo(f"report_path={payload['report_path']}")
    if payload.get("failure_reason"):
        typer.echo(f"failure_reason={payload['failure_reason']}")
    typer.echo("production_candidate_generated=false")
    if payload.get("failure_reason"):
        raise typer.Exit(code=1)


@dynamic_v3_window_audit_app.command("inspect-artifact")
def dynamic_v3_window_audit_inspect_artifact_command(
    artifact_path: Annotated[
        Path,
        typer.Option("--artifact-path", help="artifact JSON path。"),
    ],
) -> None:
    """检查单个 artifact 的 backtest window 状态。"""
    payload = inspect_window_artifact(artifact_path=artifact_path)
    record = mapping_obj(payload["record"])
    typer.echo(f"status={payload['status']}")
    typer.echo(f"artifact_type={record.get('artifact_type')}")
    typer.echo(f"configured_backtest_start={record.get('configured_backtest_start')}")
    typer.echo(f"actual_evaluation_start={record.get('actual_evaluation_start')}")
    typer.echo(f"actual_evaluation_end={record.get('actual_evaluation_end')}")
    typer.echo(f"promotion_blocking={str(record.get('promotion_blocking')).lower()}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-window-audit")
def dynamic_v3_validate_window_audit_command(
    audit_id: Annotated[str, typer.Option("--audit-id", help="window audit id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="window audit artifact root。"),
    ] = DEFAULT_WINDOW_AUDIT_DIR,
) -> None:
    """校验 TRADING-111 window audit artifacts。"""
    payload = validate_window_audit_artifact(audit_id=audit_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
