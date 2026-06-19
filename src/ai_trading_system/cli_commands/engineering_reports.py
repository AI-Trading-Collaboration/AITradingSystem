from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.artifact_lifecycle_inventory import (
    build_artifact_lifecycle_inventory_payload,
    default_artifact_lifecycle_inventory_json_path,
    default_artifact_lifecycle_inventory_markdown_path,
    default_artifact_lifecycle_inventory_validation_json_path,
    default_artifact_lifecycle_inventory_validation_markdown_path,
    latest_artifact_lifecycle_inventory_json_path,
    validate_artifact_lifecycle_inventory_payload,
    write_artifact_lifecycle_inventory_json,
    write_artifact_lifecycle_inventory_markdown,
    write_artifact_lifecycle_inventory_validation_json,
    write_artifact_lifecycle_inventory_validation_markdown,
)
from ai_trading_system.reports.canonical_system_status import resolve_latest_report_record
from ai_trading_system.reports.engineering_closeout import (
    build_engineering_surface_inventory_payload,
    default_engineering_surface_inventory_json_path,
    default_engineering_surface_inventory_markdown_path,
    default_engineering_surface_inventory_validation_json_path,
    default_engineering_surface_inventory_validation_markdown_path,
    latest_engineering_surface_inventory_json_path,
    validate_engineering_surface_inventory_payload,
    write_engineering_surface_inventory_json,
    write_engineering_surface_inventory_markdown,
    write_engineering_surface_inventory_validation_json,
    write_engineering_surface_inventory_validation_markdown,
)
from ai_trading_system.reports.engineering_release_candidate import (
    build_engineering_closeout_release_candidate_payload,
    default_engineering_closeout_release_candidate_json_path,
    default_engineering_closeout_release_candidate_markdown_path,
    default_engineering_closeout_release_candidate_validation_json_path,
    default_engineering_closeout_release_candidate_validation_markdown_path,
    latest_engineering_closeout_release_candidate_json_path,
    validate_engineering_closeout_release_candidate_payload,
    write_engineering_closeout_release_candidate_json,
    write_engineering_closeout_release_candidate_markdown,
    write_engineering_closeout_release_candidate_validation_json,
    write_engineering_closeout_release_candidate_validation_markdown,
)
from ai_trading_system.reports.engineering_stage_b_readiness import (
    DEFAULT_ENGINEERING_CLOSEOUT_POLICY_PATH,
    build_engineering_stage_b_readiness_payload,
    default_engineering_stage_b_readiness_json_path,
    default_engineering_stage_b_readiness_markdown_path,
    default_engineering_stage_b_readiness_validation_json_path,
    default_engineering_stage_b_readiness_validation_markdown_path,
    latest_engineering_stage_b_readiness_json_path,
    validate_engineering_stage_b_readiness_payload,
    write_engineering_stage_b_readiness_json,
    write_engineering_stage_b_readiness_markdown,
    write_engineering_stage_b_readiness_validation_json,
    write_engineering_stage_b_readiness_validation_markdown,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
)

console = Console()


def register_engineering_report_commands(reports_app: typer.Typer) -> None:
    reports_app.command("engineering-surface-inventory")(engineering_surface_inventory_command)
    reports_app.command("validate-engineering-surface-inventory")(
        validate_engineering_surface_inventory_command
    )
    reports_app.command("artifact-lifecycle-inventory")(artifact_lifecycle_inventory_command)
    reports_app.command("validate-artifact-lifecycle-inventory")(
        validate_artifact_lifecycle_inventory_command
    )
    reports_app.command("engineering-stage-b-readiness")(engineering_stage_b_readiness_command)
    reports_app.command("validate-engineering-stage-b-readiness")(
        validate_engineering_stage_b_readiness_command
    )
    reports_app.command("engineering-closeout-release-candidate")(
        engineering_closeout_release_candidate_command
    )
    reports_app.command("validate-engineering-closeout-release-candidate")(
        validate_engineering_closeout_release_candidate_command
    )
    reports_app.command("latest")(report_latest_command)


def engineering_surface_inventory_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Engineering surface inventory 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于扫描 CLI/config/docs/artifacts 的项目根目录。"),
    ] = PROJECT_ROOT,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path | None,
        typer.Option(help="artifact catalog Markdown 路径；不传时使用 docs/artifact_catalog.md。"),
    ] = None,
    scheduled_tasks_path: Annotated[
        Path | None,
        typer.Option(help="scheduled tasks YAML 路径；不传时使用 config/scheduled_tasks.yaml。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering surface inventory JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering surface inventory Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成工程收尾公开表面盘点；只读扫描 CLI、registry、artifact、config 和 docs。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_engineering_surface_inventory_payload(
        as_of=report_date,
        project_root=project_root,
        report_registry_path=registry_path,
        artifact_catalog_path=artifact_catalog_path,
        scheduled_tasks_path=scheduled_tasks_path,
    )
    report_json = json_output_path or default_engineering_surface_inventory_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_engineering_surface_inventory_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_engineering_surface_inventory_json(payload, report_json)
    md_path = write_engineering_surface_inventory_markdown(payload, report_md)
    status = payload["inventory_status"]
    style = "green" if status == "ENGINEERING_SURFACE_INVENTORY_READY" else "yellow"
    summary = payload["summary"]
    console.print(f"[{style}]Engineering surface inventory：{status}[/{style}]")
    console.print(f"Engineering surface inventory JSON：{json_path}")
    console.print(f"Engineering surface inventory Markdown：{md_path}")
    console.print(
        f"surfaces：{summary['surface_count']}；"
        f"unknown：{summary['unknown_surface_count']}；"
        f"warnings：{summary['warning_count']}；"
        f"production_effect={payload['production_effect']}；只读盘点"
    )


def validate_engineering_surface_inventory_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 engineering surface inventory JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Engineering surface inventory validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Engineering surface inventory JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering surface inventory validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering surface inventory validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验工程表面盘点，确保每个 surface 均有冻结分类和安全边界。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_engineering_surface_inventory_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 engineering surface inventory JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_engineering_surface_inventory_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Engineering surface inventory JSON not found: {source_path}")
    raw_payload = _read_object_json(source_path, "Engineering surface inventory")
    payload = validate_engineering_surface_inventory_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["engineering_surface_inventory"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_engineering_surface_inventory_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or default_engineering_surface_inventory_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_engineering_surface_inventory_validation_json(payload, validation_json)
    md_path = write_engineering_surface_inventory_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = _status_style(status)
    summary = payload["summary"]
    console.print(f"[{style}]Engineering surface inventory validation：{status}[/{style}]")
    console.print(f"Engineering surface inventory validation JSON：{json_path}")
    console.print(f"Engineering surface inventory validation Markdown：{md_path}")
    console.print(
        f"surfaces：{summary['surface_count']}；"
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


def artifact_lifecycle_inventory_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Artifact lifecycle inventory 日期，格式为 YYYY-MM-DD。",
        ),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于扫描 report artifacts 的项目根目录。"),
    ] = PROJECT_ROOT,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path | None,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lifecycle inventory JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lifecycle inventory Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 artifact lifecycle/latest pointer 只读盘点。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_artifact_lifecycle_inventory_payload(
        as_of=report_date,
        project_root=project_root,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )
    report_json = json_output_path or default_artifact_lifecycle_inventory_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_artifact_lifecycle_inventory_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_artifact_lifecycle_inventory_json(payload, report_json)
    md_path = write_artifact_lifecycle_inventory_markdown(payload, report_md)
    status = payload["inventory_status"]
    style = "green" if status == "ARTIFACT_LIFECYCLE_READY" else "yellow"
    if status == "ARTIFACT_LIFECYCLE_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Artifact lifecycle inventory：{status}[/{style}]")
    console.print(f"Artifact lifecycle inventory JSON：{json_path}")
    console.print(f"Artifact lifecycle inventory Markdown：{md_path}")
    console.print(
        f"reports：{summary['report_count']}；"
        f"current：{summary['current_count']}；"
        f"legacy：{summary['legacy_count']}；"
        f"invalid：{summary['invalid_count']}；"
        f"unwaived：{summary['report_index_unwaived_issue_count']}；"
        f"production_effect={payload['production_effect']}；只读盘点"
    )
    if status == "ARTIFACT_LIFECYCLE_BLOCKED":
        raise typer.Exit(code=1)


def validate_artifact_lifecycle_inventory_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 artifact lifecycle inventory JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Artifact lifecycle inventory validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lifecycle inventory JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lifecycle inventory validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Artifact lifecycle inventory validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 artifact lifecycle/latest pointer 盘点。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_artifact_lifecycle_inventory_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 artifact lifecycle inventory JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_artifact_lifecycle_inventory_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Artifact lifecycle inventory JSON not found: {source_path}")
    raw_payload = _read_object_json(source_path, "Artifact lifecycle inventory")
    payload = validate_artifact_lifecycle_inventory_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["artifact_lifecycle_inventory"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_artifact_lifecycle_inventory_validation_json_path(reports_dir, report_date)
    )
    validation_md = (
        markdown_output_path
        or default_artifact_lifecycle_inventory_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_artifact_lifecycle_inventory_validation_json(payload, validation_json)
    md_path = write_artifact_lifecycle_inventory_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = _status_style(status)
    summary = payload["summary"]
    console.print(f"[{style}]Artifact lifecycle inventory validation：{status}[/{style}]")
    console.print(f"Artifact lifecycle inventory validation JSON：{json_path}")
    console.print(f"Artifact lifecycle inventory validation Markdown：{md_path}")
    console.print(
        f"records：{summary['artifact_record_count']}；"
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


def engineering_stage_b_readiness_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Engineering Stage B readiness 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于扫描 artifacts/config/runtime/test contracts 的项目根目录。"),
    ] = PROJECT_ROOT,
    policy_path: Annotated[
        Path,
        typer.Option(help="engineering_closeout_policy.yaml 路径。"),
    ] = DEFAULT_ENGINEERING_CLOSEOUT_POLICY_PATH,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path | None,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering Stage B readiness JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering Stage B readiness Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 Stage B schema/config/reproducibility/test/error taxonomy 只读 readiness。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_engineering_stage_b_readiness_payload(
        as_of=report_date,
        project_root=project_root,
        policy_path=policy_path,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )
    report_json = json_output_path or default_engineering_stage_b_readiness_json_path(
        reports_dir,
        report_date,
    )
    report_md = markdown_output_path or default_engineering_stage_b_readiness_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_engineering_stage_b_readiness_json(payload, report_json)
    md_path = write_engineering_stage_b_readiness_markdown(payload, report_md)
    status = payload["readiness_status"]
    style = "green" if status == "ENGINEERING_STAGE_B_READY" else "yellow"
    if status == "ENGINEERING_STAGE_B_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Engineering Stage B readiness：{status}[/{style}]")
    console.print(f"Engineering Stage B readiness JSON：{json_path}")
    console.print(f"Engineering Stage B readiness Markdown：{md_path}")
    console.print(
        f"latest_json：{summary['latest_json_artifact_count']}；"
        f"schema_missing：{summary['latest_json_schema_version_missing_count']}；"
        f"manifest_missing_fields：{summary['run_manifest_missing_required_field_count']}；"
        f"missing_tiers：{summary['missing_validation_tier_count']}；"
        f"production_effect={payload['production_effect']}；只读 readiness"
    )
    if status == "ENGINEERING_STAGE_B_BLOCKED":
        raise typer.Exit(code=1)


def validate_engineering_stage_b_readiness_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 engineering Stage B readiness JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Engineering Stage B validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Engineering Stage B readiness JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering Stage B validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering Stage B validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 Stage B readiness，不把 warning backlog 冒充为平台冻结完成。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_engineering_stage_b_readiness_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 engineering Stage B readiness JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_engineering_stage_b_readiness_json_path(reports_dir, report_date)
    if not source_path.exists():
        raise typer.BadParameter(f"Engineering Stage B readiness JSON not found: {source_path}")
    raw_payload = _read_object_json(source_path, "Engineering Stage B readiness")
    payload = validate_engineering_stage_b_readiness_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["engineering_stage_b_readiness"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_engineering_stage_b_readiness_validation_json_path(reports_dir, report_date)
    )
    validation_md = (
        markdown_output_path
        or default_engineering_stage_b_readiness_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_engineering_stage_b_readiness_validation_json(payload, validation_json)
    md_path = write_engineering_stage_b_readiness_validation_markdown(payload, validation_md)
    status = payload["validation_status"]
    style = _status_style(status)
    summary = payload["summary"]
    console.print(f"[{style}]Engineering Stage B readiness validation：{status}[/{style}]")
    console.print(f"Engineering Stage B readiness validation JSON：{json_path}")
    console.print(f"Engineering Stage B readiness validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


def engineering_closeout_release_candidate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Engineering closeout release 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    validation_runtime_dir: Annotated[
        Path,
        typer.Option(help="validation runtime artifact 根目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "validation_runtime",
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录。"),
    ] = PROJECT_ROOT,
    release_tag: Annotated[
        str | None,
        typer.Option(help="拟定或已创建的 release tag。"),
    ] = None,
    release_version: Annotated[
        str | None,
        typer.Option(help="拟定 release version。"),
    ] = None,
    allow_dirty_worktree: Annotated[
        bool,
        typer.Option(help="允许 dirty worktree 只生成 BLOCKED 审计 artifact。"),
    ] = False,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering closeout release candidate JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Engineering closeout release candidate Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成平台冻结 release candidate 只读审计报告。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    payload = build_engineering_closeout_release_candidate_payload(
        as_of=report_date,
        project_root=project_root,
        reports_dir=reports_dir,
        validation_runtime_dir=validation_runtime_dir,
        release_tag=release_tag,
        release_version=release_version,
        enforce_git_clean=not allow_dirty_worktree,
    )
    report_json = json_output_path or default_engineering_closeout_release_candidate_json_path(
        reports_dir,
        report_date,
    )
    report_md = (
        markdown_output_path
        or default_engineering_closeout_release_candidate_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_engineering_closeout_release_candidate_json(payload, report_json)
    md_path = write_engineering_closeout_release_candidate_markdown(payload, report_md)
    status = payload["closeout_status"]
    style = "green" if status == "ENGINEERING_CLOSEOUT_READY" else "yellow"
    if status == "ENGINEERING_CLOSEOUT_BLOCKED":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Engineering closeout release candidate：{status}[/{style}]")
    console.print(f"Engineering closeout release candidate JSON：{json_path}")
    console.print(f"Engineering closeout release candidate Markdown：{md_path}")
    console.print(
        f"checks：{summary['completion_check_count']}；"
        f"blocking：{summary['blocking_issue_count']}；"
        f"warnings：{summary['warning_issue_count']}；"
        f"production_effect={payload['production_effect']}；只读 release 审计"
    )
    if status == "ENGINEERING_CLOSEOUT_BLOCKED":
        raise typer.Exit(code=1)


def validate_engineering_closeout_release_candidate_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 reports_dir 中最新 closeout release candidate JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Engineering closeout release validation 日期。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    source_json_path: Annotated[
        Path | None,
        typer.Option(help="Closeout release candidate JSON 路径；优先于 --latest/--as-of。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Closeout release validation JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Closeout release validation Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验平台冻结 release candidate，不把 BLOCKED 状态冒充为 READY。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if source_json_path is not None:
        source_path = source_json_path
    elif latest:
        latest_path = latest_engineering_closeout_release_candidate_json_path(reports_dir)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 closeout release candidate JSON：{reports_dir}")
        source_path = latest_path
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        source_path = default_engineering_closeout_release_candidate_json_path(
            reports_dir,
            report_date,
        )
    if not source_path.exists():
        raise typer.BadParameter(f"Closeout release candidate JSON not found: {source_path}")
    raw_payload = _read_object_json(source_path, "Engineering closeout release candidate")
    payload = validate_engineering_closeout_release_candidate_payload(raw_payload)
    source_artifacts = dict(payload.get("input_artifacts", {}))
    source_artifacts["engineering_closeout_release_candidate"] = str(source_path)
    payload["input_artifacts"] = source_artifacts
    report_date = _parse_date(str(payload.get("as_of") or date.today().isoformat()))
    validation_json = (
        json_output_path
        or default_engineering_closeout_release_candidate_validation_json_path(
            reports_dir,
            report_date,
        )
    )
    validation_md = (
        markdown_output_path
        or default_engineering_closeout_release_candidate_validation_markdown_path(
            reports_dir,
            report_date,
        )
    )
    json_path = write_engineering_closeout_release_candidate_validation_json(
        payload,
        validation_json,
    )
    md_path = write_engineering_closeout_release_candidate_validation_markdown(
        payload,
        validation_md,
    )
    status = payload["validation_status"]
    style = _status_style(status)
    summary = payload["summary"]
    console.print(f"[{style}]Engineering closeout release validation：{status}[/{style}]")
    console.print(f"Engineering closeout release validation JSON：{json_path}")
    console.print(f"Engineering closeout release validation Markdown：{md_path}")
    console.print(
        f"checks：{summary['check_count']}；"
        f"failed：{summary['failed_check_count']}；"
        f"warnings：{summary['warning_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )
    if status == "FAIL":
        raise typer.Exit(code=1)


def report_latest_command(
    report_id: Annotated[
        str,
        typer.Option("--report-id", help="report_registry.yaml 中的 report_id。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report index 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path | None,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    project_root: Annotated[
        Path,
        typer.Option(help="用于扫描 report artifacts 的项目根目录。"),
    ] = PROJECT_ROOT,
) -> None:
    """查询 report registry 中某个 report_id 的 latest artifact。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        lookup = resolve_latest_report_record(
            report_id=report_id,
            as_of=report_date,
            project_root=project_root,
            registry_path=registry_path,
            waiver_path=waiver_path,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    report = lookup["report"]
    status = lookup["status"]
    style = "green" if status == "FOUND" else "yellow"
    if status == "REPORT_ID_NOT_REGISTERED":
        style = "red"
    console.print(f"[{style}]Report latest：{status}[/{style}]")
    console.print(f"report_id={report_id}")
    console.print(f"freshness={report.get('freshness_status', 'UNKNOWN')}")
    console.print(f"artifact_status={report.get('artifact_status', 'UNKNOWN')}")
    console.print(f"visibility={report.get('visibility_status', 'UNKNOWN')}")
    console.print(f"latest_artifact_name={report.get('latest_artifact_name', '')}")
    console.print(f"latest_artifact={report.get('latest_artifact_path', '')}")
    console.print(f"command={report.get('command', '')}")
    console.print(f"owner_action={report.get('owner_action', '')}")
    console.print("production_effect=none；只读 latest 查询，不运行上游。")
    if status == "REPORT_ID_NOT_REGISTERED":
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _read_object_json(path: Path, artifact_label: str) -> dict[str, object]:
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{artifact_label} JSON cannot be parsed: {path}") from exc
    if not isinstance(raw_payload, dict):
        raise typer.BadParameter(f"{artifact_label} JSON must be an object: {path}")
    return raw_payload


def _status_style(status: str) -> str:
    if status == "FAIL":
        return "red"
    if status == "PASS":
        return "green"
    return "yellow"
