from __future__ import annotations

import ast
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = 1
REPORT_TYPE = "engineering_surface_inventory"
VALIDATION_REPORT_TYPE = "engineering_surface_inventory_validation"
PRODUCTION_EFFECT = "none"

PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"
INVENTORY_READY = "ENGINEERING_SURFACE_INVENTORY_READY"
INVENTORY_READY_WITH_LIMITATIONS = "ENGINEERING_SURFACE_INVENTORY_READY_WITH_LIMITATIONS"
VALID_CLASSIFICATIONS = frozenset(
    {
        "KEEP",
        "MERGE",
        "DEPRECATE",
        "ARCHIVE",
        "REMOVE_AFTER_COMPATIBILITY_WINDOW",
    }
)
REQUIRED_SURFACE_TYPES = (
    "cli_top_level",
    "cli_command",
    "report_registry_entry",
    "artifact_catalog_family",
    "configuration_file",
    "operations_entry_point",
    "documentation_family",
)
LEGACY_TOP_LEVEL_COMMANDS = frozenset({"features", "regime", "report", "run", "experiments"})
TASK_SPECIFIC_REPORT_PATTERNS = (
    "candidate",
    "dynamic-v3",
    "next-candidate",
    "next-research",
    "recovery",
    "return-to-research",
    "decision-stage",
    "owner-",
)


def default_engineering_surface_inventory_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"engineering_surface_inventory_{as_of.isoformat()}.json"


def default_engineering_surface_inventory_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"engineering_surface_inventory_{as_of.isoformat()}.md"


def default_engineering_surface_inventory_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"engineering_surface_inventory_validation_{as_of.isoformat()}.json"


def default_engineering_surface_inventory_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"engineering_surface_inventory_validation_{as_of.isoformat()}.md"


def latest_engineering_surface_inventory_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "engineering_surface_inventory_", ".json")


def build_engineering_surface_inventory_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path | None = None,
    scheduled_tasks_path: Path | None = None,
) -> dict[str, Any]:
    catalog_path = artifact_catalog_path or project_root / "docs" / "artifact_catalog.md"
    schedule_path = scheduled_tasks_path or project_root / "config" / "scheduled_tasks.yaml"
    surfaces: list[dict[str, Any]] = []
    surfaces.extend(_collect_cli_surfaces(project_root))
    surfaces.extend(_collect_report_registry_surfaces(report_registry_path))
    surfaces.extend(_collect_artifact_catalog_surfaces(catalog_path, project_root))
    surfaces.extend(_collect_config_surfaces(project_root))
    surfaces.extend(_collect_operations_surfaces(project_root, schedule_path))
    surfaces.extend(_collect_documentation_surfaces(project_root))
    surfaces = _dedupe_surfaces(surfaces)
    type_counts = Counter(_text(surface.get("surface_type")) for surface in surfaces)
    classification_counts = Counter(_text(surface.get("classification")) for surface in surfaces)
    warnings = _inventory_warnings(surfaces=surfaces, type_counts=type_counts)
    status = INVENTORY_READY_WITH_LIMITATIONS if warnings else INVENTORY_READY
    devex_paths = {
        "module_manifest": project_root / "inputs/architecture/arch_004e_module_manifest.yaml",
        "test_manifest": project_root / "inputs/architecture/arch_004e_test_manifest.yaml",
        "architecture_fitness": (
            project_root / "inputs/architecture/arch_004e_architecture_fitness.yaml"
        ),
    }
    devex_control = {
        artifact_id: _generated_devex_summary(path) for artifact_id, path in devex_paths.items()
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "inventory_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": "not_applicable_engineering_surface_inventory",
        "purpose": (
            "Inventory exposed engineering surfaces before platform closeout and freeze scope."
        ),
        "input_artifacts": {
            "cli_entrypoint": str(project_root / "src" / "ai_trading_system" / "cli.py"),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(catalog_path),
            "scheduled_tasks": str(schedule_path),
            "operations_runbook": str(
                project_root / "docs" / "operations" / "operations_runbook.md"
            ),
            "task_register": str(project_root / "docs" / "task_register.md"),
            **{artifact_id: str(path) for artifact_id, path in devex_paths.items()},
        },
        "classification_policy": {
            "allowed_values": sorted(VALID_CLASSIFICATIONS),
            "keep": "Stable public surface or source-of-truth artifact family.",
            "merge": (
                "Useful capability that should move behind canonical workflow/status entrypoints."
            ),
            "deprecate": "Supported for compatibility while a replacement is documented.",
            "archive": (
                "Historical evidence or old task context retained for audit, not a live workflow."
            ),
            "remove_after_compatibility_window": (
                "Compatibility alias or duplicate surface that may be removed after users migrate."
            ),
        },
        "summary": {
            "surface_count": len(surfaces),
            "surface_type_counts": dict(sorted(type_counts.items())),
            "classification_counts": dict(sorted(classification_counts.items())),
            "unknown_surface_count": len(
                [
                    surface
                    for surface in surfaces
                    if _text(surface.get("classification")) not in VALID_CLASSIFICATIONS
                ]
            ),
            "required_surface_type_count": len(REQUIRED_SURFACE_TYPES),
            "present_required_surface_type_count": len(
                [
                    surface_type
                    for surface_type in REQUIRED_SURFACE_TYPES
                    if type_counts[surface_type]
                ]
            ),
            "warning_count": len(warnings),
            "owned_module_count": devex_control["module_manifest"].get("module_count", 0),
            "classified_test_file_count": devex_control["test_manifest"].get("test_file_count", 0),
            "architecture_fitness_status": devex_control["architecture_fitness"].get(
                "status", "NOT_GENERATED"
            ),
        },
        "surface_type_coverage": [
            {
                "surface_type": surface_type,
                "count": type_counts[surface_type],
                "coverage_status": "PRESENT" if type_counts[surface_type] else "MISSING",
            }
            for surface_type in REQUIRED_SURFACE_TYPES
        ],
        "surfaces": surfaces,
        "warnings": warnings,
        "source_artifacts": [
            _source_artifact(
                "cli_entrypoint",
                project_root / "src" / "ai_trading_system" / "cli.py",
            ),
            _source_artifact("report_registry", report_registry_path),
            _source_artifact("artifact_catalog", catalog_path),
            _source_artifact("scheduled_tasks", schedule_path),
            _source_artifact(
                "operations_runbook",
                project_root / "docs" / "operations" / "operations_runbook.md",
            ),
            _source_artifact("task_register", project_root / "docs" / "task_register.md"),
            *(
                _source_artifact(artifact_id, path, required=False)
                for artifact_id, path in devex_paths.items()
            ),
        ],
        "generated_devex_control_plane": devex_control,
        "reader_brief": {
            "summary": f"工程表面盘点状态为 {status}，公开 surface={len(surfaces)}。",
            "key_result": status,
            "classification_counts": dict(sorted(classification_counts.items())),
            "warnings": [warning["warning_id"] for warning in warnings],
            "safety_boundary": (
                "read_existing_project_metadata_only; production_effect=none; "
                "no strategy logic, broker/order, official target weights, or production mutation."
            ),
            "next_action": (
                "use_inventory_as_scope_for_canonical_cli_status_artifact_lifecycle_closeout"
            ),
        },
        "methodology": {
            "collector_mode": "read_existing_project_metadata_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_strategy_logic": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
            "generated_manifests_linked_when_available": True,
        },
    }


def validate_engineering_surface_inventory_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    surfaces = _records(payload.get("surfaces"))
    surface_types = Counter(_text(surface.get("surface_type")) for surface in surfaces)
    invalid_classifications = [
        _text(surface.get("surface_id"))
        for surface in surfaces
        if _text(surface.get("classification")) not in VALID_CLASSIFICATIONS
    ]
    missing_required_types = [
        surface_type for surface_type in REQUIRED_SURFACE_TYPES if not surface_types[surface_type]
    ]
    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        severity="BLOCKING",
        message=f"report_type must be {REPORT_TYPE}.",
        recommended_action="rerun_engineering_surface_inventory",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect_none",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        severity="BLOCKING",
        message="engineering surface inventory must be production_effect=none.",
        recommended_action="regenerate_without_production_mutation",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="surfaces_present",
        passed=bool(surfaces),
        severity="BLOCKING",
        message="inventory must contain at least one surface.",
        recommended_action="fix_inventory_collector_inputs",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="surface_classifications_known",
        passed=not invalid_classifications,
        severity="BLOCKING",
        message="every surface must use an approved freeze classification.",
        recommended_action="assign_keep_merge_deprecate_archive_or_remove_after_window",
        details={"invalid_surface_ids": invalid_classifications},
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="required_surface_types_present",
        passed=not missing_required_types,
        severity="BLOCKING",
        message="inventory must cover CLI, reports, artifacts, config, docs, and operations.",
        recommended_action="extend_inventory_collector_for_missing_surface_types",
        details={"missing_surface_types": missing_required_types},
    )
    source_artifacts = _records(payload.get("source_artifacts"))
    missing_sources = [
        _text(item.get("artifact_id"))
        for item in source_artifacts
        if item.get("required") is True and item.get("exists") is not True
    ]
    _append_check(
        checks,
        warning_issues,
        check_id="required_source_artifacts_exist",
        passed=not missing_sources,
        severity="WARNING",
        message="some expected source artifacts are missing from this workspace.",
        recommended_action="review_missing_source_before_freeze_scope_signoff",
        details={"missing_source_artifacts": missing_sources},
    )
    warning_count = len(_records(payload.get("warnings")))
    _append_check(
        checks,
        warning_issues,
        check_id="inventory_warnings_visible",
        passed=warning_count == 0,
        severity="WARNING",
        message="inventory surfaced limitations that need closeout follow-up.",
        recommended_action="review_inventory_warnings_before_platform_freeze",
        details={"warning_count": warning_count},
    )
    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of")),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "source_inventory_status": _text(
            payload.get("inventory_status"),
            _text(payload.get("status")),
        ),
        "input_artifacts": _mapping(payload.get("input_artifacts")),
        "source_artifacts": _mapping(payload.get("input_artifacts")),
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(
                [check for check in checks if check["status"] == FAIL_STATUS]
            ),
            "warning_check_count": len(
                [check for check in checks if check["status"] == WARN_STATUS]
            ),
            "blocking_issue_count": len(blocking_issues),
            "warning_issue_count": len(warning_issues),
            "surface_count": len(surfaces),
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": f"工程表面盘点校验状态为 {status}。",
            "key_result": status,
            "blocking_issues": [issue["issue_id"] for issue in blocking_issues],
            "warnings": [issue["issue_id"] for issue in warning_issues],
            "safety_boundary": "validation_reads_inventory_only; production_effect=none.",
            "next_action": (
                "resolve_inventory_validation_blockers"
                if status == FAIL_STATUS
                else "continue_engineering_closeout_sequence"
            ),
        },
        "methodology": {
            "mode": "read_engineering_surface_inventory_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_engineering_surface_inventory_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_engineering_surface_inventory_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_engineering_surface_inventory_markdown(payload), encoding="utf-8")
    return output_path


def write_engineering_surface_inventory_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_engineering_surface_inventory_json(payload, output_path)


def write_engineering_surface_inventory_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_engineering_surface_inventory_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_engineering_surface_inventory_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    surfaces = _records(payload.get("surfaces"))
    lines = [
        f"# Engineering Surface Inventory {payload.get('as_of')}",
        "",
        "## Reader Brief",
        "",
        f"- Summary：{_text(_mapping(payload.get('reader_brief')).get('summary'))}",
        f"- Key Result：{_text(_mapping(payload.get('reader_brief')).get('key_result'))}",
        f"- Safety Boundary：{_text(_mapping(payload.get('reader_brief')).get('safety_boundary'))}",
        f"- Next Action：{_text(_mapping(payload.get('reader_brief')).get('next_action'))}",
        "",
        "## Summary",
        "",
        f"- 状态：{_text(payload.get('inventory_status'))}",
        f"- production_effect：{_text(payload.get('production_effect'))}",
        f"- market_regime：{_text(payload.get('market_regime'))}",
        f"- requested_date_range：{_text(payload.get('requested_date_range'))}",
        f"- surface_count：{summary.get('surface_count')}",
        f"- unknown_surface_count：{summary.get('unknown_surface_count')}",
        f"- warning_count：{summary.get('warning_count')}",
        "",
        "## Classification Counts",
        "",
        "|classification|count|",
        "|---|---|",
    ]
    for key, value in _mapping(summary.get("classification_counts")).items():
        lines.append(f"|{_markdown_cell(key)}|{_markdown_cell(value)}|")
    lines.extend(
        [
            "",
            "## Surface Type Coverage",
            "",
            "|surface_type|count|coverage_status|",
            "|---|---|---|",
        ]
    )
    for item in _records(payload.get("surface_type_coverage")):
        lines.append(
            f"|{_markdown_cell(item.get('surface_type'))}|"
            f"{_markdown_cell(item.get('count'))}|"
            f"{_markdown_cell(item.get('coverage_status'))}|"
        )
    lines.extend(
        [
            "",
            "## Freeze Scope",
            "",
            "|surface_id|type|classification|owner_action|path|",
            "|---|---|---|---|---|",
        ]
    )
    for surface in surfaces:
        lines.append(
            f"|{_markdown_cell(surface.get('surface_id'))}|"
            f"{_markdown_cell(surface.get('surface_type'))}|"
            f"{_markdown_cell(surface.get('classification'))}|"
            f"{_markdown_cell(surface.get('owner_action'))}|"
            f"{_markdown_cell(surface.get('path'))}|"
        )
    lines.extend(
        [
            "",
            "## Warnings",
            "",
            "|warning_id|message|recommended_action|",
            "|---|---|---|",
        ]
    )
    warnings = _records(payload.get("warnings"))
    for warning in warnings:
        lines.append(
            f"|{_markdown_cell(warning.get('warning_id'))}|"
            f"{_markdown_cell(warning.get('message'))}|"
            f"{_markdown_cell(warning.get('recommended_action'))}|"
        )
    if not warnings:
        lines.append("|NONE|无 warning。||")
    lines.extend(
        [
            "",
            "## Methodology",
            "",
            "本报告只读扫描仓库中的 CLI、report registry、artifact catalog、config、docs "
            "和 operations 入口；"
            "不运行上游报告、不刷新数据、不修改策略逻辑、不写 official target weights、"
            "不触发 broker/order 或 production mutation。",
            "",
        ]
    )
    return "\n".join(lines)


def render_engineering_surface_inventory_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Engineering Surface Inventory Validation {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('validation_status'))}",
        f"- production_effect：{_text(payload.get('production_effect'))}",
        f"- source_inventory_status：{_text(payload.get('source_inventory_status'))}",
        f"- surfaces：{summary.get('surface_count')}",
        f"- checks：{summary.get('check_count')}",
        f"- failed_checks：{summary.get('failed_check_count')}",
        f"- warnings：{summary.get('warning_check_count')}",
        "",
        "## Checks",
        "",
        "|check_id|status|severity|message|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_markdown_cell(check.get('check_id'))}|"
            f"{_markdown_cell(check.get('status'))}|"
            f"{_markdown_cell(check.get('severity'))}|"
            f"{_markdown_cell(check.get('message'))}|"
        )
    lines.append("")
    return "\n".join(lines)


def _collect_cli_surfaces(project_root: Path) -> list[dict[str, Any]]:
    cli_path = project_root / "src" / "ai_trading_system" / "cli.py"
    surfaces: list[dict[str, Any]] = []
    if cli_path.exists():
        text = cli_path.read_text(encoding="utf-8")
        for line in text.splitlines():
            match = re.search(r"app\.add_typer\(.*?name=[\"']([^\"']+)[\"']", line)
            if match is None:
                continue
            name = match.group(1)
            classification = (
                "REMOVE_AFTER_COMPATIBILITY_WINDOW" if name in LEGACY_TOP_LEVEL_COMMANDS else "KEEP"
            )
            surfaces.append(
                _surface(
                    surface_type="cli_top_level",
                    name=name,
                    path=cli_path,
                    classification=classification,
                    owner_action=(
                        "migrate_users_to_canonical_etf_or_reports_entrypoint"
                        if classification == "REMOVE_AFTER_COMPATIBILITY_WINDOW"
                        else "retain_as_public_cli_group"
                    ),
                    rationale=(
                        "compatibility alias from older ETF command layout"
                        if classification == "REMOVE_AFTER_COMPATIBILITY_WINDOW"
                        else "registered public Typer group"
                    ),
                    command=f"aits {name}",
                )
            )
    command_files = sorted(
        (project_root / "src" / "ai_trading_system" / "cli_commands").glob("*.py")
    )
    for path in command_files:
        for command_name, app_name in _command_decorators(path):
            classification = _classify_cli_command(command_name, path)
            surfaces.append(
                _surface(
                    surface_type="cli_command",
                    name=f"{path.stem}:{command_name}",
                    path=path,
                    classification=classification,
                    owner_action=_cli_owner_action(classification, command_name),
                    rationale=f"Typer command on {app_name}",
                    command=command_name,
                )
            )
    return surfaces


def _collect_report_registry_surfaces(registry_path: Path) -> list[dict[str, Any]]:
    surfaces: list[dict[str, Any]] = []
    try:
        registry = load_report_registry(registry_path)
    except (FileNotFoundError, ValueError):
        return surfaces
    for entry in _records(registry.get("reports")):
        report_id = _text(entry.get("report_id"))
        group = _text(entry.get("group"), "unknown")
        classification = "KEEP"
        owner_action = _text(entry.get("owner_action")) or "keep_registry_entry_current"
        if group in {"legacy", "deprecated"} or "legacy" in report_id:
            classification = "DEPRECATE"
            owner_action = "document_replacement_before_compatibility_removal"
        surfaces.append(
            _surface(
                surface_type="report_registry_entry",
                name=report_id,
                path=registry_path,
                classification=classification,
                owner_action=owner_action,
                rationale=(
                    f"registered report family; group={group}; "
                    f"cadence={_text(entry.get('cadence'))}"
                ),
                command=_text(entry.get("command")),
                metadata={
                    "group": group,
                    "cadence": _text(entry.get("cadence")),
                    "artifact_globs": _strings(entry.get("artifact_globs")),
                },
            )
        )
    return surfaces


def _collect_artifact_catalog_surfaces(
    catalog_path: Path,
    project_root: Path,
) -> list[dict[str, Any]]:
    if not catalog_path.exists():
        return []
    surfaces: list[dict[str, Any]] = []
    for index, line in enumerate(catalog_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.startswith("| `") and not line.startswith("|`"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 3 or cells[0].lower() in {"artifact", "产物"}:
            continue
        family = _catalog_family_name(cells[0], index)
        classification = "ARCHIVE" if "/requirements/" in cells[0] else "KEEP"
        surfaces.append(
            _surface(
                surface_type="artifact_catalog_family",
                name=family,
                path=catalog_path,
                classification=classification,
                owner_action=(
                    "retain_for_lineage_and_reproducibility"
                    if classification == "KEEP"
                    else "retain_as_historical_audit_context"
                ),
                rationale=f"artifact catalog row {index}",
                command=_strip_markdown(cells[1]) if len(cells) > 1 else "",
                metadata={"line_number": index},
            )
        )
    if not surfaces:
        surfaces.append(
            _surface(
                surface_type="artifact_catalog_family",
                name="artifact_catalog_document",
                path=catalog_path,
                classification="KEEP",
                owner_action="review_catalog_parser_if_family_rows_missing",
                rationale="artifact catalog exists but no table rows were parsed",
            )
        )
    if (project_root / "docs" / "artifact_catalog.md").resolve() != catalog_path.resolve():
        surfaces.append(
            _surface(
                surface_type="artifact_catalog_family",
                name="custom_artifact_catalog_path",
                path=catalog_path,
                classification="KEEP",
                owner_action="use_custom_catalog_for_inventory_test_or_manual_review",
                rationale="non-default artifact catalog input",
            )
        )
    return surfaces


def _collect_config_surfaces(project_root: Path) -> list[dict[str, Any]]:
    surfaces: list[dict[str, Any]] = []
    for path in sorted((project_root / "config").rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".yaml", ".yml", ".toml", ".json"}:
            continue
        surfaces.append(
            _surface(
                surface_type="configuration_file",
                name=path.relative_to(project_root).as_posix(),
                path=path,
                classification="KEEP",
                owner_action="retain_as_reviewed_configuration_surface",
                rationale="configuration file under config/",
                metadata={"schema_version": _schema_version(path)},
            )
        )
    env_example = project_root / ".env.example"
    if env_example.exists():
        surfaces.append(
            _surface(
                surface_type="configuration_file",
                name=".env.example",
                path=env_example,
                classification="KEEP",
                owner_action="retain_as_environment_setup_contract",
                rationale="environment variable template",
            )
        )
    return surfaces


def _collect_operations_surfaces(
    project_root: Path,
    scheduled_tasks_path: Path,
) -> list[dict[str, Any]]:
    surfaces: list[dict[str, Any]] = []
    if scheduled_tasks_path.exists():
        surfaces.append(
            _surface(
                surface_type="operations_entry_point",
                name="scheduled_tasks",
                path=scheduled_tasks_path,
                classification="KEEP",
                owner_action="retain_as_unified_periodic_task_registry",
                rationale="canonical scheduled task configuration",
            )
        )
        try:
            schedule = safe_load_yaml_path(scheduled_tasks_path)
        except ValueError:
            schedule = {}
        for cadence in _schedule_cadences(schedule):
            surfaces.append(
                _surface(
                    surface_type="operations_entry_point",
                    name=f"scheduled_tasks:{cadence}",
                    path=scheduled_tasks_path,
                    classification="KEEP",
                    owner_action="route_periodic_execution_through_documented_daily_entrypoint",
                    rationale="cadence registered in config/scheduled_tasks.yaml",
                    metadata={"cadence": cadence},
                )
            )
    runbook = project_root / "docs" / "operations" / "operations_runbook.md"
    if runbook.exists():
        surfaces.append(
            _surface(
                surface_type="operations_entry_point",
                name="operations_runbook",
                path=runbook,
                classification="KEEP",
                owner_action="read_before_periodic_operations",
                rationale="periodic operations source runbook",
            )
        )
    for path in sorted((project_root / "scripts").glob("*scheduler*.py")):
        surfaces.append(
            _surface(
                surface_type="operations_entry_point",
                name=path.name,
                path=path,
                classification="DEPRECATE",
                owner_action="confirm_script_is_template_or_route_through_unified_scheduler",
                rationale="scheduler helper script outside canonical daily-run entrypoint",
            )
        )
    return surfaces


def _collect_documentation_surfaces(project_root: Path) -> list[dict[str, Any]]:
    docs_root = project_root / "docs"
    surfaces: list[dict[str, Any]] = []
    top_level_docs = sorted(path for path in docs_root.glob("*.md") if path.is_file())
    for path in top_level_docs:
        name = path.relative_to(project_root).as_posix()
        classification = "KEEP"
        owner_action = "retain_as_active_project_documentation"
        if (
            path.name.startswith("runbook_")
            and (docs_root / "operations" / "operations_runbook.md").exists()
        ):
            classification = "MERGE"
            owner_action = "link_or_merge_into_docs_operations_operations_runbook"
        surfaces.append(
            _surface(
                surface_type="documentation_family",
                name=name,
                path=path,
                classification=classification,
                owner_action=owner_action,
                rationale="top-level Markdown documentation",
            )
        )
    requirements_root = docs_root / "requirements"
    if requirements_root.exists():
        requirement_count = len([path for path in requirements_root.glob("*.md") if path.is_file()])
        surfaces.append(
            _surface(
                surface_type="documentation_family",
                name="docs/requirements",
                path=requirements_root,
                classification="ARCHIVE",
                owner_action="retain_for_task_history_and_move_reader_guides_to_information_architecture",
                rationale="task-level requirement history, not primary user entrypoint",
                metadata={"document_count": requirement_count},
            )
        )
    operations_root = docs_root / "operations"
    if operations_root.exists():
        surfaces.append(
            _surface(
                surface_type="documentation_family",
                name="docs/operations",
                path=operations_root,
                classification="KEEP",
                owner_action="retain_as_operations_entry_documentation",
                rationale="operations documentation family",
            )
        )
    return surfaces


def _command_decorators(path: Path) -> list[tuple[str, str]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return []
    commands: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            command = _decorator_command(decorator)
            if command is not None:
                commands.append(command)
    return commands


def _decorator_command(node: ast.AST) -> tuple[str, str] | None:
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if not isinstance(func, ast.Attribute) or func.attr != "command":
        return None
    if not isinstance(func.value, ast.Name):
        return None
    app_name = func.value.id
    command_name = ""
    if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
        command_name = node.args[0].value
    if not command_name:
        for keyword in node.keywords:
            if (
                keyword.arg == "name"
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, str)
            ):
                command_name = keyword.value.value
                break
    return (command_name or "callback", app_name)


def _classify_cli_command(command_name: str, path: Path) -> str:
    if path.stem == "etf_compat":
        return "REMOVE_AFTER_COMPATIBILITY_WINDOW"
    if path.stem == "reports" and any(
        pattern in command_name for pattern in TASK_SPECIFIC_REPORT_PATTERNS
    ):
        return "MERGE"
    return "KEEP"


def _cli_owner_action(classification: str, command_name: str) -> str:
    if classification == "REMOVE_AFTER_COMPATIBILITY_WINDOW":
        return "keep_as_deprecation_alias_until_canonical_workflow_compatibility_window_ends"
    if classification == "MERGE":
        return f"route_{command_name}_behind_canonical_status_or_research_workflow"
    return "retain_public_command_and_document_owner_workflow"


def _schedule_cadences(schedule: object) -> list[str]:
    if not isinstance(schedule, Mapping):
        return []
    tasks = schedule.get("tasks")
    if isinstance(tasks, Mapping):
        return sorted(_text(key) for key in tasks if _text(key))
    cadences = schedule.get("cadences")
    if isinstance(cadences, Mapping):
        return sorted(_text(key) for key in cadences if _text(key))
    return []


def _schema_version(path: Path) -> str:
    if path.suffix.lower() not in {".yaml", ".yml", ".json"}:
        return ""
    try:
        if path.suffix.lower() == ".json":
            raw = json.loads(path.read_text(encoding="utf-8"))
        else:
            raw = safe_load_yaml_path(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return ""
    if isinstance(raw, Mapping):
        return _text(raw.get("schema_version"))
    return ""


def _catalog_family_name(cell: str, index: int) -> str:
    cleaned = _strip_markdown(cell)
    if not cleaned:
        return f"artifact_catalog_row_{index}"
    first = cleaned.split("<br/>", 1)[0].split("\n", 1)[0]
    return first[:160]


def _surface(
    *,
    surface_type: str,
    name: str,
    path: Path,
    classification: str,
    owner_action: str,
    rationale: str,
    command: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_name = _normalize_id(name)
    surface_id = f"{surface_type}:{normalized_name}"
    return {
        "surface_id": surface_id,
        "surface_type": surface_type,
        "name": name,
        "path": str(path),
        "command": command,
        "classification": classification,
        "owner_action": owner_action,
        "rationale": rationale,
        "metadata": {} if metadata is None else dict(metadata),
        "production_effect": PRODUCTION_EFFECT,
    }


def _dedupe_surfaces(surfaces: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for surface in surfaces:
        surface_id = _text(surface.get("surface_id"))
        if not surface_id or surface_id in seen:
            continue
        seen.add(surface_id)
        deduped.append(dict(surface))
    return deduped


def _inventory_warnings(
    *,
    surfaces: Sequence[Mapping[str, Any]],
    type_counts: Counter[str],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for surface_type in REQUIRED_SURFACE_TYPES:
        if not type_counts[surface_type]:
            warnings.append(
                {
                    "warning_id": f"{surface_type}_missing",
                    "message": f"required surface type {surface_type} was not discovered.",
                    "recommended_action": "extend_inventory_inputs_before_freeze_scope_signoff",
                }
            )
    merge_count = len([surface for surface in surfaces if surface.get("classification") == "MERGE"])
    if merge_count:
        warnings.append(
            {
                "warning_id": "merge_scope_not_empty",
                "message": (
                    f"{merge_count} surfaces should be consolidated behind canonical workflows."
                ),
                "recommended_action": "use_inventory_to_drive_canonical_cli_and_status_tasks",
            }
        )
    remove_count = len(
        [
            surface
            for surface in surfaces
            if surface.get("classification") == "REMOVE_AFTER_COMPATIBILITY_WINDOW"
        ]
    )
    if remove_count:
        warnings.append(
            {
                "warning_id": "compatibility_aliases_present",
                "message": f"{remove_count} compatibility aliases remain visible.",
                "recommended_action": "publish_deprecation_messages_before_removal_window",
            }
        )
    return warnings


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    *,
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    status = PASS_STATUS if passed else WARN_STATUS if severity == "WARNING" else FAIL_STATUS
    check = {
        "check_id": check_id,
        "status": status,
        "severity": severity,
        "message": message,
        "recommended_action": recommended_action,
        "details": {} if details is None else dict(details),
    }
    checks.append(check)
    if passed:
        return
    issues.append(
        {
            "issue_id": check_id,
            "severity": severity,
            "scope": "engineering_surface_inventory",
            "message": message,
            "recommended_action": recommended_action,
            "details": check["details"],
        }
    )


def _source_artifact(artifact_id: str, path: Path, *, required: bool = True) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": path.exists(),
        "required": required,
        "production_effect": PRODUCTION_EFFECT,
    }


def _generated_devex_summary(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"path": str(path), "status": "NOT_GENERATED", "exists": False}
    raw = safe_load_yaml_path(path)
    payload = dict(raw) if isinstance(raw, Mapping) else {}
    return {
        "path": str(path),
        "exists": True,
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "module_count": payload.get("module_count"),
        "test_file_count": payload.get("test_file_count"),
        "violation_count": payload.get("violation_count"),
    }


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    if not output_dir.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in output_dir.glob(f"{prefix}*{suffix}"):
        raw = path.stem.removeprefix(prefix)
        try:
            parsed = date.fromisoformat(raw)
        except ValueError:
            continue
        candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].name))[1]


def _strip_markdown(value: object) -> str:
    text = _text(value)
    text = text.replace("`", "")
    text = re.sub(r"<br\s*/?>", "<br/>", text)
    return text.strip()


def _normalize_id(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.:-]+", "_", value.strip())
    return normalized.strip("_")[:180] or "unnamed"


def _dedupe_issues(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for record in records:
        key = (_text(record.get("issue_id")), json.dumps(record.get("details", {}), sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(record))
    return deduped


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item not in {"", None}]


def _text(value: Any, default: str = "") -> str:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")
