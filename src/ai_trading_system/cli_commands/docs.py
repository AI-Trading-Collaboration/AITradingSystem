from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.decision_snapshots import DEFAULT_DECISION_SNAPSHOT_DIR
from ai_trading_system.docs_freshness import (
    default_docs_freshness_paths,
    validate_docs_freshness,
    write_docs_freshness_report,
)
from ai_trading_system.documentation_contract import (
    DEFAULT_ARTIFACT_CATALOG_PATH,
    build_documentation_contract_payload,
    default_documentation_contract_json_path,
    default_documentation_contract_report_path,
    write_documentation_contract_json,
    write_documentation_contract_report,
)
from ai_trading_system.heuristic_governance import (
    DEFAULT_HEURISTIC_GOVERNANCE_CONFIG_PATH,
    build_heuristic_governance_payload,
    default_heuristic_governance_json_path,
    default_heuristic_governance_report_path,
    write_heuristic_governance_json,
    write_heuristic_governance_report,
)
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH

console = Console()
docs_app = typer.Typer(help="项目文档治理和新鲜度检查。", no_args_is_help=True)


@docs_app.command("validate-freshness")
def validate_docs_freshness_command(
    paths: Annotated[
        list[Path] | None,
        typer.Option(
            "--path",
            help="要检查的新鲜度文档路径；不传则检查关键 docs 和 requirements。",
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="可选 Markdown 文档新鲜度报告输出路径。"),
    ] = None,
) -> None:
    """检查关键项目文档的 `最后更新` 是否落后于内部状态记录。"""
    checked_paths = tuple(paths or default_docs_freshness_paths(PROJECT_ROOT))
    report = validate_docs_freshness(checked_paths)
    if output_path is not None:
        write_docs_freshness_report(report, output_path)

    style = "green" if report.passed else "red"
    console.print(f"[{style}]文档新鲜度：{report.status}[/{style}]")
    console.print(f"检查文档数：{len(report.records)}")
    console.print(f"问题数：{len(report.issues)}")
    if output_path is not None:
        console.print(f"报告：{output_path}")
    for issue in report.issues[:10]:
        console.print(f"{issue.path}: {issue.code}: {issue.message}")
    if report.issues:
        raise typer.Exit(code=1)


@docs_app.command("report-contract")
def documentation_contract_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of", "--date", help="Documentation contract 日期，格式为 YYYY-MM-DD，默认今天。"
        ),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path,
        typer.Option(help="artifact_catalog.md 路径。"),
    ] = DEFAULT_ARTIFACT_CATALOG_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Documentation contract Markdown 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Documentation contract JSON 输出路径。"),
    ] = None,
    fail_on_warning: Annotated[
        bool,
        typer.Option(help="存在 warning 时也以非零退出。"),
    ] = False,
) -> None:
    """校验 report registry 是否被 artifact catalog 覆盖并生成只读文档契约。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    report_date = (
        _latest_decision_snapshot_date()
        if latest
        else (_parse_date(as_of) if as_of else date.today())
    )
    reports_dir = PROJECT_ROOT / "outputs" / "reports"
    markdown_output = output_path or default_documentation_contract_report_path(
        reports_dir,
        report_date,
    )
    json_output = json_output_path or default_documentation_contract_json_path(
        reports_dir,
        report_date,
    )
    try:
        payload = build_documentation_contract_payload(
            as_of=report_date,
            registry_path=registry_path,
            artifact_catalog_path=artifact_catalog_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report_path = write_documentation_contract_report(payload, markdown_output)
    json_path = write_documentation_contract_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    if payload["status"] == "FAIL":
        style = "red"
    console.print(f"[{style}]Documentation contract：{payload['status']}[/{style}]")
    console.print(f"Documentation contract report：{report_path}")
    console.print(f"Documentation contract JSON：{json_path}")
    console.print(
        f"reports：{payload['summary']['report_count']}；"
        f"errors：{payload['summary']['error_count']}；"
        f"warnings：{payload['summary']['warning_count']}；"
        f"production_effect={payload['production_effect']}；"
        "只读文档契约"
    )
    if payload["status"] == "FAIL" or (fail_on_warning and payload["summary"]["warning_count"]):
        raise typer.Exit(code=1)


@docs_app.command("heuristic-audit")
def heuristic_governance_audit_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Heuristic governance audit 日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(help="heuristic_governance.yaml 路径。"),
    ] = DEFAULT_HEURISTIC_GOVERNANCE_CONFIG_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Heuristic governance Markdown 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Heuristic governance JSON 输出路径。"),
    ] = None,
    fail_on_warning: Annotated[
        bool,
        typer.Option(help="存在 warning 时也以非零退出。"),
    ] = False,
) -> None:
    """扫描投资解释路径的未登记 numeric literal，并校验 policy metadata。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    reports_dir = PROJECT_ROOT / "outputs" / "reports"
    markdown_output = output_path or default_heuristic_governance_report_path(
        reports_dir,
        report_date,
    )
    json_output = json_output_path or default_heuristic_governance_json_path(
        reports_dir,
        report_date,
    )
    try:
        payload = build_heuristic_governance_payload(
            as_of=report_date,
            config_path=config_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report_path = write_heuristic_governance_report(payload, markdown_output)
    json_path = write_heuristic_governance_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    if payload["status"] == "FAIL":
        style = "red"
    console.print(f"[{style}]Heuristic governance audit：{payload['status']}[/{style}]")
    console.print(f"Heuristic governance report：{report_path}")
    console.print(f"Heuristic governance JSON：{json_path}")
    console.print(
        f"numeric_literals={payload['summary']['numeric_literal_finding_count']}；"
        f"unregistered={payload['summary']['unregistered_numeric_literal_count']}；"
        f"policy_metadata_failures={payload['summary']['failed_policy_metadata_check_count']}；"
        f"warnings={payload['summary']['warning_count']}；"
        f"production_effect={payload['production_effect']}；"
        "只读治理审计"
    )
    if payload["status"] == "FAIL" or (fail_on_warning and payload["summary"]["warning_count"]):
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc


def _latest_decision_snapshot_date(
    snapshot_dir: Path = DEFAULT_DECISION_SNAPSHOT_DIR,
) -> date:
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
    return max(candidates, key=lambda item: (item[0], item[1].name))[0]


def _decision_snapshot_date(path: Path) -> date:
    raw_date = path.stem.removeprefix("decision_snapshot_")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise typer.BadParameter(f"无法读取 decision_snapshot 日期：{path}") from exc
    if isinstance(payload, dict):
        signal_date = payload.get("signal_date") or payload.get("as_of")
        if signal_date:
            return _parse_date(str(signal_date))
    raise typer.BadParameter(f"decision_snapshot 文件名或内容缺少 YYYY-MM-DD 日期：{path}")
