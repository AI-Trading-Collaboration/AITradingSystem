from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.decision_snapshots import DEFAULT_DECISION_SNAPSHOT_DIR
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    default_report_index_html_path,
    default_report_index_json_path,
    write_report_index_html,
    write_report_index_json,
)
from ai_trading_system.reports.waiver_inventory import (
    build_waiver_inventory_payload,
    default_waiver_inventory_json_path,
    default_waiver_inventory_markdown_path,
    default_waiver_inventory_validation_json_path,
    default_waiver_inventory_validation_markdown_path,
    latest_waiver_inventory_json_path,
    validate_waiver_inventory_payload,
    write_waiver_inventory_json,
    write_waiver_inventory_markdown,
    write_waiver_inventory_validation_json,
    write_waiver_inventory_validation_markdown,
)

console = Console()


def register_report_index_commands(reports_app: typer.Typer) -> None:
    reports_app.command("index")(report_index_command)
    reports_app.command("waiver-inventory")(waiver_inventory_command)
    reports_app.command("validate-waiver-inventory")(validate_waiver_inventory_command)


def report_index_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report index 日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    project_root: Annotated[
        Path,
        typer.Option(help="用于扫描 report artifacts 的项目根目录。"),
    ] = PROJECT_ROOT,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Report index HTML 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读报告 registry / cadence index。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    reports_dir = project_root / "outputs" / "reports"
    html_output = output_path or default_report_index_html_path(reports_dir, report_date)
    json_output = json_output_path or default_report_index_json_path(reports_dir, report_date)
    try:
        payload = build_report_index_payload(
            as_of=report_date,
            project_root=project_root,
            registry_path=registry_path,
            waiver_path=waiver_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    html_path = write_report_index_html(payload, html_output)
    json_path = write_report_index_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Report index：{payload['status']}[/{style}]")
    console.print(f"Report index HTML：{html_path}")
    console.print(f"Report index JSON：{json_path}")
    console.print(
        f"reports：{payload['summary']['report_count']}；"
        f"missing：{payload['summary']['missing_count']}；"
        f"stale：{payload['summary']['stale_count']}；"
        f"waived：{payload['summary']['explicit_waiver_count']}；"
        f"expired_waivers：{payload['summary']['expired_waiver_count']}；"
        f"unwaived：{payload['summary']['unwaived_warning_count']}；"
        f"production_effect={payload['production_effect']}；"
        "只读扫描"
    )


def waiver_inventory_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Waiver inventory 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 report index waiver inventory；只读扫描 waiver policy。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        payload = build_waiver_inventory_payload(
            as_of=report_date,
            waiver_path=waiver_path,
            registry_path=registry_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    inventory_json = json_output_path or default_waiver_inventory_json_path(
        reports_dir,
        report_date,
    )
    inventory_md = markdown_output_path or default_waiver_inventory_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_waiver_inventory_json(payload, inventory_json)
    md_path = write_waiver_inventory_markdown(payload, inventory_md)
    style = "green" if payload["inventory_status"] == "PASS" else "yellow"
    if payload["inventory_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Waiver inventory：{payload['inventory_status']}[/{style}]")
    console.print(f"Waiver inventory JSON：{json_path}")
    console.print(f"Waiver inventory Markdown：{md_path}")
    console.print(
        f"waivers：{summary['expanded_waiver_count']}；"
        f"active：{summary['active_waiver_count']}；"
        f"expired：{summary['expired_waiver_count']}；"
        f"expiring_soon：{summary['expiring_soon_waiver_count']}；"
        f"production_effect={payload['production_effect']}；只读治理检查"
    )
    if payload["inventory_status"] == "FAIL":
        raise typer.Exit(code=1)


def validate_waiver_inventory_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 waiver inventory JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Waiver inventory validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory JSON 路径；优先级高于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Waiver inventory validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 waiver inventory，并在 expired waiver 存在时 fail closed。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_waiver_inventory_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 waiver inventory JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_waiver_inventory_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"waiver inventory JSON not found: {source_path}")
    raw_payload = _read_object_json(source_path, "waiver inventory")
    payload = validate_waiver_inventory_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["waiver_inventory"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = json_output_path or default_waiver_inventory_validation_json_path(
        reports_dir,
        report_date,
    )
    validation_md = markdown_output_path or default_waiver_inventory_validation_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_waiver_inventory_validation_json(payload, validation_json)
    md_path = write_waiver_inventory_validation_markdown(payload, validation_md)
    style = "green" if payload["validation_status"] == "PASS" else "yellow"
    if payload["validation_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(
        f"[{style}]Waiver inventory validation：{payload['validation_status']}[/{style}]"
    )
    console.print(f"Waiver inventory validation JSON：{json_path}")
    console.print(f"Waiver inventory validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _read_object_json(path: Path, artifact_label: str) -> dict[str, Any]:
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{artifact_label} JSON cannot be parsed: {path}") from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"{artifact_label} JSON must be an object: {path}")
    return raw_payload


def _latest_decision_snapshot_path(snapshot_dir: Path) -> Path:
    candidates: list[tuple[date, Path]] = []
    for path in snapshot_dir.glob("decision_snapshot_*.json"):
        if not path.is_file():
            continue
        try:
            candidates.append((_decision_snapshot_date(path), path))
        except typer.BadParameter:
            continue
    if not candidates:
        raise typer.BadParameter(f"未找到可用 decision_snapshot：{snapshot_dir}")
    return max(candidates, key=lambda item: item[0])[1]


def _decision_snapshot_date(path: Path) -> date:
    raw_date = path.stem.removeprefix("decision_snapshot_")
    if raw_date:
        try:
            return date.fromisoformat(raw_date)
        except ValueError:
            pass
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise typer.BadParameter(f"无法读取 decision_snapshot 日期：{path}") from exc
    if isinstance(payload, dict):
        for key in ("as_of", "date", "signal_date"):
            value = payload.get(key)
            if isinstance(value, str):
                try:
                    return date.fromisoformat(value)
                except ValueError:
                    continue
    raise typer.BadParameter(f"decision_snapshot 文件名或内容缺少 YYYY-MM-DD 日期：{path}")
