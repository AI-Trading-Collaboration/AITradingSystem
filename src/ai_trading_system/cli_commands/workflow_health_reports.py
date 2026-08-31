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
    build_workflow_health_cycle_receipt,
    build_workflow_health_payloads,
    default_workflow_candidates_json_path,
    default_workflow_health_cycle_receipt_path,
    default_workflow_health_json_path,
    default_workflow_health_markdown_path,
    default_workflow_health_validation_json_path,
    default_workflow_health_validation_markdown_path,
    latest_current_week_validated_bundle,
    latest_workflow_candidates_json_path,
    latest_workflow_health_json_path,
    load_workflow_health_policy,
    resolve_workflow_health_checkout_identity,
    validate_workflow_health_payloads,
    write_workflow_candidates_json,
    write_workflow_health_cycle_receipt,
    write_workflow_health_json,
    write_workflow_health_markdown,
    write_workflow_health_validation_json,
    write_workflow_health_validation_markdown,
)

console = Console()


def register_workflow_health_report_commands(reports_app: typer.Typer) -> None:
    reports_app.command("workflow-health")(workflow_health_command)
    reports_app.command("ensure-workflow-health")(ensure_workflow_health_command)
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
        history_dir=reports_dir,
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


def ensure_workflow_health_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="自动 gate 观测日期，格式为 YYYY-MM-DD。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="Workflow health artifact 输出目录。"),
    ] = PROJECT_ROOT / "outputs" / "reports",
    receipt_dir: Annotated[
        Path,
        typer.Option(help="自动周期 receipt 输出目录。"),
    ] = PROJECT_ROOT / "outputs" / "run_control" / "workflow_health",
    project_root: Annotated[
        Path,
        typer.Option(help="必须位于 exact main/origin identity 的 development checkout。"),
    ] = PROJECT_ROOT,
    policy_path: Annotated[
        Path,
        typer.Option(help="Workflow health reviewed policy。"),
    ] = DEFAULT_POLICY_PATH,
) -> None:
    """在 existing daily automation 中按 ISO week 自动生成或复用流程健康报告。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    receipt_path = default_workflow_health_cycle_receipt_path(receipt_dir, report_date)
    governed_paths = (
        policy_path,
        project_root / "src" / "ai_trading_system" / "reports" / "workflow_health.py",
        project_root
        / "src"
        / "ai_trading_system"
        / "cli_commands"
        / "workflow_health_reports.py",
        project_root / "config" / "scheduled_tasks.yaml",
    )
    checkout_identity, checkout_blockers = resolve_workflow_health_checkout_identity(
        project_root=project_root,
        governed_paths=governed_paths,
    )
    try:
        policy = load_workflow_health_policy(policy_path)
        automatic_policy = dict(policy["cadence"]["automatic_report_generation"])
        owner_decision_id = str(automatic_policy["owner_decision_id"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        receipt = build_workflow_health_cycle_receipt(
            as_of=report_date,
            action="BLOCKED_POLICY",
            status="BLOCKED",
            owner_decision_id="UNRESOLVED",
            checkout_identity=checkout_identity,
            blocker_codes=(f"WORKFLOW_HEALTH_POLICY_INVALID:{type(exc).__name__}",),
        )
        write_workflow_health_cycle_receipt(receipt, receipt_path)
        console.print("[red]Workflow health 自动周期：BLOCKED_POLICY[/red]")
        console.print(f"Receipt：{receipt_path}")
        raise typer.Exit(code=1) from exc

    if checkout_blockers:
        receipt = build_workflow_health_cycle_receipt(
            as_of=report_date,
            action="BLOCKED_CHECKOUT_IDENTITY",
            status="BLOCKED",
            owner_decision_id=owner_decision_id,
            checkout_identity=checkout_identity,
            blocker_codes=checkout_blockers,
        )
        write_workflow_health_cycle_receipt(receipt, receipt_path)
        console.print("[red]Workflow health 自动周期：BLOCKED_CHECKOUT_IDENTITY[/red]")
        console.print(f"Receipt：{receipt_path}")
        raise typer.Exit(code=1)

    current = latest_current_week_validated_bundle(
        reports_dir=reports_dir,
        as_of=report_date,
    )
    if current is not None:
        artifact_paths = tuple(
            Path(current[key])
            for key in (
                "report_path",
                "report_markdown_path",
                "candidate_path",
                "validation_path",
                "validation_markdown_path",
            )
        )
        receipt = build_workflow_health_cycle_receipt(
            as_of=report_date,
            action="ALREADY_CURRENT",
            status="PASS",
            owner_decision_id=owner_decision_id,
            checkout_identity=checkout_identity,
            report=current["report"],
            candidate_bundle=current["candidates"],
            validation=current["validation"],
            artifact_paths=artifact_paths,
        )
        write_workflow_health_cycle_receipt(receipt, receipt_path)
        console.print("[green]Workflow health 自动周期：ALREADY_CURRENT[/green]")
        console.print(f"Report：{current['report_path']}")
        console.print(f"Receipt：{receipt_path}")
        return

    target_paths = (
        default_workflow_health_json_path(reports_dir, report_date),
        default_workflow_health_markdown_path(reports_dir, report_date),
        default_workflow_candidates_json_path(reports_dir, report_date),
        default_workflow_health_validation_json_path(reports_dir, report_date),
        default_workflow_health_validation_markdown_path(reports_dir, report_date),
    )
    if any(path.exists() for path in target_paths):
        receipt = build_workflow_health_cycle_receipt(
            as_of=report_date,
            action="BLOCKED_INVALID_CURRENT_DATE_BUNDLE",
            status="BLOCKED",
            owner_decision_id=owner_decision_id,
            checkout_identity=checkout_identity,
            artifact_paths=tuple(path for path in target_paths if path.exists()),
            blocker_codes=("CURRENT_DATE_BUNDLE_INVALID_OR_INCOMPLETE",),
        )
        write_workflow_health_cycle_receipt(receipt, receipt_path)
        console.print("[red]Workflow health 自动周期：当前日期 bundle 无效，未覆盖[/red]")
        console.print(f"Receipt：{receipt_path}")
        raise typer.Exit(code=1)

    try:
        report, candidates = build_workflow_health_payloads(
            as_of=report_date,
            project_root=project_root,
            policy_path=policy_path,
            history_dir=reports_dir,
        )
        validation = validate_workflow_health_payloads(report, candidates)
        write_workflow_health_json(report, target_paths[0])
        write_workflow_health_markdown(report, target_paths[1])
        write_workflow_candidates_json(candidates, target_paths[2])
        write_workflow_health_validation_json(validation, target_paths[3])
        write_workflow_health_validation_markdown(validation, target_paths[4])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        receipt = build_workflow_health_cycle_receipt(
            as_of=report_date,
            action="FAILED_GENERATION",
            status="FAILED",
            owner_decision_id=owner_decision_id,
            checkout_identity=checkout_identity,
            artifact_paths=tuple(path for path in target_paths if path.exists()),
            blocker_codes=(f"WORKFLOW_HEALTH_GENERATION_EXCEPTION:{type(exc).__name__}",),
        )
        write_workflow_health_cycle_receipt(receipt, receipt_path)
        console.print("[red]Workflow health 自动周期：FAILED_GENERATION[/red]")
        console.print(f"Receipt：{receipt_path}")
        raise typer.Exit(code=1) from exc

    cycle_status = (
        "PASS"
        if validation["validation_status"] in {"PASS", "PASS_WITH_WARNINGS"}
        else "FAILED"
    )
    receipt = build_workflow_health_cycle_receipt(
        as_of=report_date,
        action="GENERATED",
        status=cycle_status,
        owner_decision_id=owner_decision_id,
        checkout_identity=checkout_identity,
        report=report,
        candidate_bundle=candidates,
        validation=validation,
        artifact_paths=target_paths,
        blocker_codes=(
            ()
            if cycle_status == "PASS"
            else ("WORKFLOW_HEALTH_INDEPENDENT_VALIDATION_FAILED",)
        ),
    )
    write_workflow_health_cycle_receipt(receipt, receipt_path)
    style = "green" if cycle_status == "PASS" else "red"
    console.print(f"[{style}]Workflow health 自动周期：{cycle_status}[/{style}]")
    console.print(f"Report：{target_paths[0]}")
    console.print(f"Receipt：{receipt_path}")
    if cycle_status != "PASS":
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
