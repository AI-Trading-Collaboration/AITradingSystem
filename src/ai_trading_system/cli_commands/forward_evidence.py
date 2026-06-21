from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.data_foundation import (
    DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
    audit_forward_evidence,
    capture_forward_evidence,
    report_forward_evidence,
    update_forward_outcomes,
)

console = Console()
forward_evidence_app = typer.Typer(help="Forward evidence capture and daily archive。")


@forward_evidence_app.command("capture-daily")
def forward_evidence_capture_daily_command(
    as_of_date: Annotated[
        str,
        typer.Option("--as-of-date", "--as-of", help="Archive as-of date。"),
    ] = "2022-12-01",
    feature_snapshot_id: Annotated[
        str,
        typer.Option("--feature-snapshot-id", help="Linked PIT feature snapshot id。"),
    ] = "pit_snapshot_required",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Forward evidence 输出目录。"),
    ] = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> None:
    payload = capture_forward_evidence(
        as_of_date=as_of_date,
        feature_snapshot_id=feature_snapshot_id,
        output_root=output_root,
    )
    _print_payload(payload)


@forward_evidence_app.command("update-outcomes")
def forward_evidence_update_outcomes_command(
    archive_id: Annotated[str, typer.Option("--archive-id", help="Archive id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Forward evidence 输出目录。"),
    ] = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> None:
    payload = update_forward_outcomes(archive_id=archive_id, output_root=output_root)
    _print_payload(payload)


@forward_evidence_app.command("audit")
def forward_evidence_audit_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Forward evidence 输出目录。"),
    ] = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> None:
    payload = audit_forward_evidence(output_root=output_root)
    _print_payload(payload)


@forward_evidence_app.command("report")
def forward_evidence_report_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Forward evidence 输出目录。"),
    ] = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> None:
    payload = report_forward_evidence(output_root=output_root)
    _print_payload(payload)


def _print_payload(payload: dict[str, object]) -> None:
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if "WARNING" in status else "red"
    console.print(
        f"[{style}]{payload.get('title', payload.get('report_type'))}：{status}[/{style}]"
    )
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in sorted(summary):
            console.print(f"{key}={summary[key]}")
    console.print("production_effect=none；broker_action=none")
