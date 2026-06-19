from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.canonical_system_status import (
    build_canonical_system_status_payload,
    default_canonical_system_doctor_json_path,
    default_canonical_system_doctor_markdown_path,
    default_canonical_system_status_json_path,
    default_canonical_system_status_markdown_path,
    validate_canonical_system_status_payload,
    write_canonical_system_doctor_json,
    write_canonical_system_doctor_markdown,
    write_canonical_system_status_json,
    write_canonical_system_status_markdown,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
)

console = Console()
system_app = typer.Typer(help="Canonical system status, doctor, and workflow entrypoints.")


@system_app.command("status")
def system_status_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="系统状态日期，格式为 YYYY-MM-DD。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="status artifact 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录，用于扫描既有 report artifacts。"),
    ] = PROJECT_ROOT,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path | None,
        typer.Option(help="report index waiver YAML 路径；传空则不应用 waiver。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical system status JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical system status Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 canonical first-screen system status bundle；只读，不运行上游。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_canonical_system_status_payload(
        as_of=report_date,
        project_root=project_root,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )
    json_path = json_output_path or default_canonical_system_status_json_path(
        reports_dir,
        report_date,
    )
    markdown_path = markdown_output_path or default_canonical_system_status_markdown_path(
        reports_dir,
        report_date,
    )
    write_canonical_system_status_json(payload, json_path)
    write_canonical_system_status_markdown(payload, markdown_path)
    first_screen = payload["first_screen"]
    style = "green" if payload["system_status"].endswith("READY") else "yellow"
    if "BLOCKED" in payload["system_status"]:
        style = "red"
    console.print(f"[{style}]System status：{payload['system_status']}[/{style}]")
    console.print(f"Status JSON：{json_path}")
    console.print(f"Status Markdown：{markdown_path}")
    console.print(f"research_gate={first_screen['latest_research_gate']}")
    console.print(f"data_health={first_screen['data_health']}")
    console.print(f"validation_health={first_screen['validation_health']}")
    console.print(f"next_action={first_screen['recommended_next_action']}")
    console.print("production_effect=none；只读系统状态，不运行上游、不触发 broker/order。")
    if "BLOCKED" in payload["system_status"]:
        raise typer.Exit(code=1)


@system_app.command("doctor")
def system_doctor_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="系统 doctor 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="doctor artifact 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录，用于扫描既有 report artifacts。"),
    ] = PROJECT_ROOT,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path | None,
        typer.Option(help="report index waiver YAML 路径；传空则不应用 waiver。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="已有 canonical_system_status JSON 路径；不传则现场生成。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical system doctor JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Canonical system doctor Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 canonical system status；blocking check fail closed。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    if source_json_path is None:
        status_payload = build_canonical_system_status_payload(
            as_of=report_date,
            project_root=project_root,
            registry_path=registry_path,
            waiver_path=waiver_path,
        )
        status_json_path = default_canonical_system_status_json_path(reports_dir, report_date)
        status_markdown_path = default_canonical_system_status_markdown_path(
            reports_dir,
            report_date,
        )
        status_payload.setdefault("input_artifacts", {})
        status_payload["input_artifacts"]["canonical_system_status"] = str(status_json_path)
        write_canonical_system_status_json(status_payload, status_json_path)
        write_canonical_system_status_markdown(status_payload, status_markdown_path)
    else:
        status_json_path = source_json_path
        if not status_json_path.exists():
            raise typer.BadParameter(f"canonical system status JSON not found: {status_json_path}")
        try:
            raw_payload = json.loads(status_json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(
                f"canonical system status JSON cannot be parsed: {status_json_path}"
            ) from exc
        if not isinstance(raw_payload, dict):
            raise typer.BadParameter(
                f"canonical system status JSON must be an object: {status_json_path}"
            )
        status_payload = raw_payload
        status_payload.setdefault("input_artifacts", {})
        status_payload["input_artifacts"]["canonical_system_status"] = str(status_json_path)
        report_date = _parse_date(str(status_payload.get("as_of") or report_date.isoformat()))

    doctor_payload = validate_canonical_system_status_payload(status_payload)
    doctor_json = json_output_path or default_canonical_system_doctor_json_path(
        reports_dir,
        report_date,
    )
    doctor_md = markdown_output_path or default_canonical_system_doctor_markdown_path(
        reports_dir,
        report_date,
    )
    write_canonical_system_doctor_json(doctor_payload, doctor_json)
    write_canonical_system_doctor_markdown(doctor_payload, doctor_md)
    style = "green" if doctor_payload["validation_status"] == "PASS" else "yellow"
    if doctor_payload["validation_status"] == "FAIL":
        style = "red"
    summary = doctor_payload["summary"]
    console.print(
        f"[{style}]System doctor：{doctor_payload['validation_status']}[/{style}]"
    )
    console.print(f"Doctor JSON：{doctor_json}")
    console.print(f"Doctor Markdown：{doctor_md}")
    console.print(
        f"checks={summary['check_count']}；failed={summary['failed_check_count']}；"
        f"warnings={summary['warning_check_count']}；production_effect=none"
    )
    if doctor_payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("date must use YYYY-MM-DD") from exc
