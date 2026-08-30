from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.workflow_health import (
    DEFAULT_POLICY_PATH,
    build_workflow_health_payloads,
    default_workflow_candidates_json_path,
    default_workflow_health_json_path,
    default_workflow_health_markdown_path,
    default_workflow_health_validation_json_path,
    default_workflow_health_validation_markdown_path,
    latest_workflow_candidates_json_path,
    latest_workflow_health_json_path,
    validate_workflow_health_payloads,
    write_workflow_candidates_json,
    write_workflow_health_json,
    write_workflow_health_markdown,
    write_workflow_health_validation_json,
    write_workflow_health_validation_markdown,
)

console = Console()


def register_workflow_health_report_commands(reports_app: typer.Typer) -> None:
    reports_app.command("workflow-health")(workflow_health_command)
    reports_app.command("validate-workflow-health")(validate_workflow_health_command)


def workflow_health_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="研发流程健康周报日期，格式为 YYYY-MM-DD。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 输出目录。"),
    ] = PROJECT_ROOT / "outputs" / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="读取 validation/publication/Git evidence 的项目根目录。"),
    ] = PROJECT_ROOT,
    policy_path: Annotated[
        Path,
        typer.Option(help="workflow health reviewed policy 路径。"),
    ] = DEFAULT_POLICY_PATH,
) -> None:
    """生成并校验只读研发流程健康周报与 review-only 优化候选。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    report, candidates = build_workflow_health_payloads(
        as_of=report_date,
        project_root=project_root,
        policy_path=policy_path,
    )
    validation = validate_workflow_health_payloads(report, candidates)
    report_json = write_workflow_health_json(
        report, default_workflow_health_json_path(reports_dir, report_date)
    )
    report_markdown = write_workflow_health_markdown(
        report, default_workflow_health_markdown_path(reports_dir, report_date)
    )
    candidate_json = write_workflow_candidates_json(
        candidates, default_workflow_candidates_json_path(reports_dir, report_date)
    )
    validation_json = write_workflow_health_validation_json(
        validation, default_workflow_health_validation_json_path(reports_dir, report_date)
    )
    validation_markdown = write_workflow_health_validation_markdown(
        validation, default_workflow_health_validation_markdown_path(reports_dir, report_date)
    )
    style = "green" if validation["validation_status"] == "PASS" else "yellow"
    if validation["validation_status"] == "FAIL":
        style = "red"
    console.print(f"[{style}]研发流程健康周报：{report['status']}[/{style}]")
    console.print(f"Report JSON：{report_json}")
    console.print(f"Report Markdown：{report_markdown}")
    console.print(f"Candidates JSON：{candidate_json}")
    console.print(f"Validation JSON：{validation_json}")
    console.print(f"Validation Markdown：{validation_markdown}")
    console.print(
        f"validation summaries={report['metrics']['validation']['summary_count']}；"
        f"transactions={report['metrics']['publication']['transaction_count']}；"
        f"candidates={candidates['candidate_count']}；"
        "production_effect=none；不会自动修改任务、代码或门禁"
    )
    if validation["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


def validate_workflow_health_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 workflow health report/candidate bundle。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="要校验的 workflow health 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT / "outputs" / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Workflow health report JSON；优先于 --latest/--as-of。"),
    ] = None,
    candidate_json_path: Annotated[
        Path | None,
        typer.Option(help="Optimization candidates JSON；不传时按 source as-of 解析。"),
    ] = None,
) -> None:
    """校验 workflow health report/candidate binding 与禁止自动执行边界。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        report_path = source_json_path
    elif latest:
        report_path = latest_workflow_health_json_path(reports_dir)
        if report_path is None:
            raise typer.BadParameter(f"未找到 workflow health JSON：{reports_dir}")
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        report_path = default_workflow_health_json_path(reports_dir, report_date)
    if not report_path.exists():
        raise typer.BadParameter(f"Workflow health JSON not found: {report_path}")
    report = _read_object_json(report_path, "Workflow health")
    report_date = _parse_date(str(report.get("as_of") or ""))
    if candidate_json_path is not None:
        candidates_path = candidate_json_path
    elif latest and source_json_path is None:
        candidates_path = latest_workflow_candidates_json_path(reports_dir)
        if candidates_path is None:
            raise typer.BadParameter(f"未找到 workflow optimization candidates JSON：{reports_dir}")
    else:
        candidates_path = default_workflow_candidates_json_path(reports_dir, report_date)
    if not candidates_path.exists():
        raise typer.BadParameter(f"Workflow candidates JSON not found: {candidates_path}")
    candidates = _read_object_json(candidates_path, "Workflow optimization candidates")
    validation = validate_workflow_health_payloads(report, candidates)
    validation["input_artifacts"] = {
        "workflow_health": str(report_path),
        "workflow_optimization_candidates": str(candidates_path),
    }
    validation_json = write_workflow_health_validation_json(
        validation, default_workflow_health_validation_json_path(reports_dir, report_date)
    )
    validation_markdown = write_workflow_health_validation_markdown(
        validation, default_workflow_health_validation_markdown_path(reports_dir, report_date)
    )
    style = "green" if validation["validation_status"] == "PASS" else "yellow"
    if validation["validation_status"] == "FAIL":
        style = "red"
    console.print(
        f"[{style}]Workflow health validation：{validation['validation_status']}[/{style}]"
    )
    console.print(f"Validation JSON：{validation_json}")
    console.print(f"Validation Markdown：{validation_markdown}")
    console.print(
        f"checks={validation['check_count']}；failed={validation['failed_check_count']}；"
        f"warnings={validation['warning_check_count']}；production_effect=none"
    )
    if validation["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须为 YYYY-MM-DD") from exc


def _read_object_json(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise typer.BadParameter(f"{label} JSON 无法读取：{path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise typer.BadParameter(f"{label} JSON 顶层必须为 object：{path}")
    return payload
