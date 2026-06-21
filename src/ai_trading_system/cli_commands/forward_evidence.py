from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.current_subscription_qualification import (
    DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    DEFAULT_FORWARD_CAPTURE_CONTRACT_PATH,
    DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT,
    DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
    DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    capture_forward_evidence_dry_run_archive,
    classify_forward_evidence_requirement,
    validate_forward_capture_contract,
)
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


@forward_evidence_app.command("capture-dry-run")
def forward_evidence_capture_dry_run_command(
    benchmark_report: Annotated[
        Path,
        typer.Option("--benchmark-report", help="TRADING-760 benchmark report JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit report JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    feature_snapshot_reference: Annotated[
        str,
        typer.Option("--feature-snapshot-reference", help="PIT feature snapshot reference。"),
    ] = "pit_snapshot_required",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-760 forward dry-run archive 输出目录。"),
    ] = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT,
) -> None:
    payload = capture_forward_evidence_dry_run_archive(
        benchmark_report_path=benchmark_report,
        control_audit_path=control_audit,
        feature_snapshot_reference=feature_snapshot_reference,
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


@forward_evidence_app.command("classify-requirement")
def forward_evidence_classify_requirement_command(
    source_requirement_matrix: Annotated[
        Path,
        typer.Option("--source-requirement-matrix", help="TRADING-737 requirement matrix JSON。"),
    ] = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-743 reclassification 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = classify_forward_evidence_requirement(
        source_requirement_matrix_path=source_requirement_matrix,
        output_root=output_root,
    )
    _print_payload(payload)


@forward_evidence_app.command("validate-capture-contract")
def forward_evidence_validate_capture_contract_command(
    capture_contract: Annotated[
        Path,
        typer.Option("--capture-contract", help="Forward evidence capture contract JSON。"),
    ] = DEFAULT_FORWARD_CAPTURE_CONTRACT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-743 contract validation 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = validate_forward_capture_contract(
        capture_contract_path=capture_contract,
        output_root=output_root,
    )
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
