from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "task_register_consistency"
VALIDATION_REPORT_TYPE = "task_register_consistency_validation"
PRODUCTION_EFFECT = "none"

PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

VALID_TASK_STATUSES = frozenset(
    {
        "PROPOSED",
        "READY",
        "IN_PROGRESS",
        "BLOCKED_OWNER_INPUT",
        "BLOCKED_EXTERNAL",
        "BASELINE_DONE",
        "VALIDATING",
        "DONE",
        "DEFERRED",
        "DROPPED",
    }
)
TERMINAL_TASK_STATUSES = frozenset({"DONE", "DROPPED"})
ACTIVE_BASELINE_TASK_STATUSES = frozenset({"BASELINE_DONE"})
REQUIRED_REPORT_IDS = (
    "task_register_consistency",
    "task_register_consistency_validation",
)
TASK_ID_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*-\d+[A-Za-z0-9_-]*$")


@dataclass(frozen=True)
class TaskRegisterRow:
    task_id: str
    base_task_id: str
    domain: str
    priority: str
    status: str
    next_owner: str
    blocker_or_next_step: str
    acceptance_criteria: str
    notes: str
    source: str
    line_number: int
    docs_links: tuple[str, ...]


def default_task_register_consistency_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"task_register_consistency_{as_of.isoformat()}.json"


def default_task_register_consistency_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"task_register_consistency_{as_of.isoformat()}.md"


def default_task_register_consistency_validation_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"task_register_consistency_validation_{as_of.isoformat()}.json"


def default_task_register_consistency_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"task_register_consistency_validation_{as_of.isoformat()}.md"


def latest_task_register_consistency_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "task_register_consistency_", ".json")


def build_task_register_consistency_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    task_register_path: Path | None = None,
    completed_register_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path | None = None,
) -> dict[str, Any]:
    task_path = task_register_path or project_root / "docs" / "task_register.md"
    completed_path = completed_register_path or project_root / "docs" / "task_register_completed.md"
    catalog_path = artifact_catalog_path or project_root / "docs" / "artifact_catalog.md"

    active_rows = parse_task_register_rows(task_path, source="active")
    completed_rows = parse_task_register_rows(completed_path, source="completed")
    all_rows = [*active_rows, *completed_rows]
    report_registry = load_report_registry(report_registry_path)
    report_ids = {
        _text(entry.get("report_id"))
        for entry in _records(report_registry.get("reports"))
        if _text(entry.get("report_id"))
    }

    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []

    _check_task_ids(checks, blocking_issues, rows=all_rows)
    _check_task_statuses(checks, blocking_issues, rows=all_rows)
    _check_active_completed_boundaries(
        checks,
        blocking_issues,
        active_rows=active_rows,
        completed_rows=completed_rows,
    )
    _check_docs_links(
        checks,
        blocking_issues,
        rows=all_rows,
        project_root=project_root,
    )
    _check_report_registry_entries(
        checks,
        blocking_issues,
        report_ids=report_ids,
        report_registry=report_registry,
    )
    _check_artifact_family(
        checks,
        blocking_issues,
        catalog_path=catalog_path,
        report_registry=report_registry,
    )
    _check_reader_brief_entry(checks, blocking_issues, report_ids=report_ids)

    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    summary = {
        "active_task_count": len(active_rows),
        "active_baseline_task_count": len(
            [row for row in active_rows if row.status in ACTIVE_BASELINE_TASK_STATUSES]
        ),
        "completed_task_count": len(completed_rows),
        "total_task_count": len(all_rows),
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "warning_check_count": len([check for check in checks if check["status"] == WARN_STATUS]),
        "blocking_issue_count": len(blocking_issues),
        "warning_issue_count": len(warning_issues),
        "explicit_docs_link_count": sum(len(row.docs_links) for row in all_rows),
        "required_report_entry_count": len(REQUIRED_REPORT_IDS),
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "consistency_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": (
            "Validate consistency across active task register, completed task register, "
            "explicit documentation links, report registry entries, artifact catalog "
            "visibility, and Reader Brief entry."
        ),
        "input_artifacts": {
            "task_register": str(task_path),
            "completed_register": str(completed_path),
            "report_registry": str(report_registry_path),
            "artifact_catalog": str(catalog_path),
        },
        "output_decision": status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "The checker validates Markdown table structure and explicit links only.",
            "It does not infer undocumented owner intent from chat history.",
            "It does not run upstream report commands or repair missing artifacts.",
        ],
        "next_action": _next_action(status),
        "summary": summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "task_registers": {
            "active": [_row_payload(row) for row in active_rows],
            "completed": [_row_payload(row) for row in completed_rows],
        },
        "report_registry_checks": {
            "required_report_ids": list(REQUIRED_REPORT_IDS),
            "reader_brief_report_id_present": "reader_brief" in report_ids,
            "registered_required_report_ids": [
                report_id for report_id in REQUIRED_REPORT_IDS if report_id in report_ids
            ],
        },
        "reader_brief": _reader_brief(status, summary, blocking_issues, warning_issues),
        "source_artifacts": [
            _source_artifact("task_register", task_path),
            _source_artifact("task_register_completed", completed_path),
            _source_artifact("report_registry", report_registry_path),
            _source_artifact("artifact_catalog", catalog_path),
        ],
        "methodology": {
            "collector_mode": "read_existing_governance_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_task_registers": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
            "valid_task_statuses": sorted(VALID_TASK_STATUSES),
            "terminal_task_statuses": sorted(TERMINAL_TASK_STATUSES),
            "active_baseline_task_statuses": sorted(ACTIVE_BASELINE_TASK_STATUSES),
        },
    }
    return payload


def validate_task_register_consistency_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []

    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        severity="BLOCKING",
        message=f"report_type must be {REPORT_TYPE}.",
        recommended_action="rerun_task_register_consistency_run_with_supported_report_type",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        severity="BLOCKING",
        message="task register consistency report must be production_effect=none.",
        recommended_action="regenerate_report_without_production_mutation",
    )
    source_status = _text(payload.get("consistency_status"), _text(payload.get("status")))
    _append_check(
        checks,
        blocking_issues,
        check_id="source_consistency_status_not_fail",
        passed=source_status != FAIL_STATUS,
        severity="BLOCKING",
        message=f"source consistency_status is {source_status}.",
        recommended_action="resolve_blocking_task_register_consistency_issues",
    )
    source_blocking = _records(payload.get("blocking_issues"))
    _append_check(
        checks,
        blocking_issues,
        check_id="source_blocking_issues_empty",
        passed=not source_blocking,
        severity="BLOCKING",
        message="source task register consistency report has blocking issues.",
        recommended_action="resolve_source_blocking_issues_before_validation_pass",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="reader_brief_summary_present",
        passed=bool(_mapping(payload.get("reader_brief"))),
        severity="WARNING",
        message="task register consistency report should include reader_brief summary.",
        recommended_action="regenerate_report_with_reader_brief_summary",
    )
    source_warnings = _records(payload.get("warning_issues"))
    if source_warnings or source_status == WARN_STATUS:
        _append_check(
            checks,
            warning_issues,
            check_id="source_warning_issues_visible",
            passed=False,
            severity="WARNING",
            message="source task register consistency report has warning issues.",
            recommended_action="review_warning_issues_before_next_governance_cycle",
        )

    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    failed_check_count = len([check for check in checks if check["status"] == FAIL_STATUS])
    warning_check_count = len([check for check in checks if check["status"] == WARN_STATUS])
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of")),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": "Validate the task register consistency report and fail closed on blockers.",
        "input_artifacts": _mapping(payload.get("input_artifacts")),
        "source_artifacts": _mapping(payload.get("input_artifacts")),
        "source_consistency_status": source_status,
        "output_decision": status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation reads the generated task register consistency report only.",
            "Validation does not edit task registers, report registry, or source documents.",
        ],
        "next_action": _next_action(status),
        "summary": {
            "check_count": len(checks),
            "failed_check_count": failed_check_count,
            "warning_check_count": warning_check_count,
            "blocking_issue_count": len(blocking_issues),
            "warning_issue_count": len(warning_issues),
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(status, {}, blocking_issues, warning_issues),
        "methodology": {
            "mode": "read_existing_consistency_report_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_task_registers": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def parse_task_register_rows(path: Path, *, source: str) -> list[TaskRegisterRow]:
    if not path.exists():
        return []
    rows: list[TaskRegisterRow] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.startswith("|") or line.startswith("|---") or line.startswith("|ID|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 8:
            continue
        task_id = cells[0]
        if not task_id or task_id == "---":
            continue
        row_text = "|".join(cells)
        rows.append(
            TaskRegisterRow(
                task_id=task_id,
                base_task_id=_base_task_id(task_id),
                domain=cells[1],
                priority=cells[2],
                status=cells[3],
                next_owner=cells[4],
                blocker_or_next_step=cells[5],
                acceptance_criteria=cells[6],
                notes=cells[7],
                source=source,
                line_number=line_number,
                docs_links=tuple(_docs_links(row_text)),
            )
        )
    return rows


def write_task_register_consistency_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_task_register_consistency_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_task_register_consistency_markdown(payload), encoding="utf-8")
    return output_path


def write_task_register_consistency_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_task_register_consistency_json(payload, output_path)


def write_task_register_consistency_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_task_register_consistency_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_task_register_consistency_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    reader = _mapping(payload.get("reader_brief"))
    methodology = _mapping(payload.get("methodology"))
    terminal_statuses = ", ".join(_strings(methodology.get("terminal_task_statuses")))
    active_baseline_statuses = ", ".join(
        _strings(methodology.get("active_baseline_task_statuses"))
    )
    lines = [
        f"# Task Register Consistency {payload.get('as_of')}",
        "",
        "## Reader Brief",
        "",
        f"- Summary：{_text(reader.get('summary'))}",
        f"- Key Result：{_text(reader.get('key_result'))}",
        f"- Blocking Issues：{_text(reader.get('blocking_issues'))}",
        f"- Warnings：{_text(reader.get('warnings'))}",
        f"- Safety Boundary：{_text(reader.get('safety_boundary'))}",
        f"- Next Action：{_text(reader.get('next_action'))}",
        "",
        "## Summary",
        "",
        f"- 状态：{_text(payload.get('consistency_status'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- active_task_count：{summary.get('active_task_count')}",
        f"- completed_task_count：{summary.get('completed_task_count')}",
        f"- checks：{summary.get('check_count')}",
        f"- failed_checks：{summary.get('failed_check_count')}",
        f"- blocking_issues：{summary.get('blocking_issue_count')}",
        f"- warning_issues：{summary.get('warning_issue_count')}",
        f"- next_action：{_text(payload.get('next_action'))}",
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
    lines.extend(
        [
            "",
            "## Blocking Issues",
            "",
            "|issue_id|scope|task_id|message|recommended_action|",
            "|---|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("blocking_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("blocking_issues")):
        lines.append("|NONE|task_register_consistency||无阻断项。||")
    lines.extend(
        [
            "",
            "## Warning Issues",
            "",
            "|issue_id|scope|task_id|message|recommended_action|",
            "|---|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("warning_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("warning_issues")):
        lines.append("|NONE|task_register_consistency||无 warning。||")
    lines.extend(
        [
            "",
            "## Methodology",
            "",
            f"- terminal task statuses：{terminal_statuses}",
            f"- active baseline task statuses：{active_baseline_statuses}",
            "本报告只读取 task register、completed register、report registry 和 artifact catalog；"
            "不运行上游命令、不刷新数据、不修改任务登记或 production state。",
            "",
        ]
    )
    return "\n".join(lines)


def render_task_register_consistency_validation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Task Register Consistency Validation {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('validation_status'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- source_consistency_status：{_text(payload.get('source_consistency_status'))}",
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
    lines.extend([""])
    return "\n".join(lines)


def _check_task_ids(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    rows: Sequence[TaskRegisterRow],
) -> None:
    missing_ids = [row for row in rows if not row.task_id]
    malformed = [row for row in rows if not TASK_ID_PATTERN.match(row.task_id)]
    duplicates = _duplicate_values([row.task_id for row in rows])
    _append_check(
        checks,
        blocking_issues,
        check_id="task_id_exists",
        passed=not missing_ids,
        severity="BLOCKING",
        message="Every task register row must include a task id.",
        recommended_action="add_missing_task_ids_before_governance_review",
        details={"missing_row_count": len(missing_ids)},
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="task_id_format",
        passed=not malformed,
        severity="BLOCKING",
        message="Task ids must include a stable numeric prefix such as TRADING-362.",
        recommended_action="rename_malformed_task_ids_to_stable_ids",
        details={"malformed_task_ids": [row.task_id for row in malformed]},
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="task_id_unique",
        passed=not duplicates,
        severity="BLOCKING",
        message="Task ids must be unique across active and completed registers.",
        recommended_action="deduplicate_task_ids_before_status_interpretation",
        details={"duplicate_task_ids": duplicates},
    )


def _check_task_statuses(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    rows: Sequence[TaskRegisterRow],
) -> None:
    invalid = [row for row in rows if row.status not in VALID_TASK_STATUSES]
    _append_check(
        checks,
        blocking_issues,
        check_id="task_status_valid",
        passed=not invalid,
        severity="BLOCKING",
        message="Every task status must be one of the documented status values.",
        recommended_action="replace_invalid_task_status_values",
        details={"invalid_statuses": [f"{row.task_id}:{row.status}" for row in invalid]},
    )


def _check_active_completed_boundaries(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    active_rows: Sequence[TaskRegisterRow],
    completed_rows: Sequence[TaskRegisterRow],
) -> None:
    active_terminal = [row for row in active_rows if row.status in TERMINAL_TASK_STATUSES]
    completed_nonterminal = [
        row for row in completed_rows if row.status not in TERMINAL_TASK_STATUSES
    ]
    completed_ids = {row.task_id for row in completed_rows}
    duplicated = [row for row in active_rows if row.task_id in completed_ids]
    _append_check(
        checks,
        blocking_issues,
        check_id="completed_tasks_not_active",
        passed=not active_terminal,
        severity="BLOCKING",
        message="Terminal tasks must be archived out of docs/task_register.md.",
        recommended_action="move_terminal_active_rows_to_completed_register",
        details={"active_terminal_task_ids": [row.task_id for row in active_terminal]},
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="completed_register_terminal_only",
        passed=not completed_nonterminal,
        severity="BLOCKING",
        message="Completed register must only contain DONE or DROPPED rows.",
        recommended_action="move_nonterminal_completed_rows_back_to_active_register",
        details={
            "completed_nonterminal_task_ids": [
                f"{row.task_id}:{row.status}" for row in completed_nonterminal
            ]
        },
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="active_tasks_not_duplicated_in_completed",
        passed=not duplicated,
        severity="BLOCKING",
        message="Active tasks must not also appear in completed register.",
        recommended_action="remove_duplicate_active_or_completed_task_row",
        details={"duplicated_task_ids": [row.task_id for row in duplicated]},
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="archived_completed_tasks_not_missing",
        passed=not active_terminal,
        severity="BLOCKING",
        message="Any task marked DONE or DROPPED must be archived.",
        recommended_action="archive_terminal_rows_in_completed_register",
        details={"missing_completed_archive_task_ids": [row.task_id for row in active_terminal]},
    )


def _check_docs_links(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    rows: Sequence[TaskRegisterRow],
    project_root: Path,
) -> None:
    missing_links: list[str] = []
    for row in rows:
        for link in row.docs_links:
            if not (project_root / link).exists():
                missing_links.append(f"{row.task_id}:{link}")
    _append_check(
        checks,
        blocking_issues,
        check_id="docs_links_exist",
        passed=not missing_links,
        severity="BLOCKING",
        message="Every explicit docs/*.md task-register link must resolve.",
        recommended_action="create_missing_docs_or_fix_task_register_links",
        details={"missing_docs_links": missing_links},
    )


def _check_report_registry_entries(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    report_ids: set[str],
    report_registry: Mapping[str, Any],
) -> None:
    missing = [report_id for report_id in REQUIRED_REPORT_IDS if report_id not in report_ids]
    _append_check(
        checks,
        blocking_issues,
        check_id="report_registry_entries_exist",
        passed=not missing,
        severity="BLOCKING",
        message="Task register consistency reports must have report registry entries.",
        recommended_action="add_missing_task_register_consistency_report_registry_entries",
        details={"missing_report_ids": missing},
    )
    entries = {
        _text(entry.get("report_id")): entry
        for entry in _records(report_registry.get("reports"))
        if _text(entry.get("report_id")) in REQUIRED_REPORT_IDS
    }
    missing_globs = [
        report_id
        for report_id, entry in entries.items()
        if not _strings(entry.get("artifact_globs"))
    ]
    _append_check(
        checks,
        blocking_issues,
        check_id="report_registry_artifact_globs_exist",
        passed=not missing_globs,
        severity="BLOCKING",
        message="Task register consistency registry entries must include artifact_globs.",
        recommended_action="add_artifact_globs_to_registry_entries",
        details={"report_ids_without_artifact_globs": missing_globs},
    )


def _check_artifact_family(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    catalog_path: Path,
    report_registry: Mapping[str, Any],
) -> None:
    entries = [
        entry
        for entry in _records(report_registry.get("reports"))
        if _text(entry.get("report_id")) in REQUIRED_REPORT_IDS
    ]
    missing_group = [
        _text(entry.get("report_id")) for entry in entries if not _text(entry.get("group"))
    ]
    catalog_text = catalog_path.read_text(encoding="utf-8") if catalog_path.exists() else ""
    catalog_has_family = all(report_id in catalog_text for report_id in REQUIRED_REPORT_IDS)
    _append_check(
        checks,
        blocking_issues,
        check_id="artifact_family_exists",
        passed=not missing_group and catalog_has_family,
        severity="BLOCKING",
        message="Task register consistency artifacts must have a registry group and catalog entry.",
        recommended_action="add_governance_group_and_artifact_catalog_entries",
        details={
            "report_ids_without_group": missing_group,
            "artifact_catalog_path": str(catalog_path),
            "catalog_has_required_report_ids": catalog_has_family,
        },
    )


def _check_reader_brief_entry(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    report_ids: set[str],
) -> None:
    _append_check(
        checks,
        blocking_issues,
        check_id="reader_brief_exists",
        passed="reader_brief" in report_ids,
        severity="BLOCKING",
        message="Reader Brief report registry entry must exist.",
        recommended_action="restore_reader_brief_report_registry_entry",
    )


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
            "scope": "task_register_consistency",
            "task_id": "",
            "message": message,
            "recommended_action": recommended_action,
            "details": check["details"],
        }
    )


def _reader_brief(
    status: str,
    summary: Mapping[str, Any],
    blocking_issues: Sequence[Mapping[str, Any]],
    warning_issues: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            "Task register、completed register、report registry 和 Reader Brief 可见性"
            f"一致性状态为 {status}。"
        ),
        "key_result": status,
        "blocking_issues": [_text(issue.get("issue_id")) for issue in blocking_issues],
        "warnings": [_text(issue.get("issue_id")) for issue in warning_issues],
        "safety_boundary": (
            "read_existing_governance_artifacts_only; production_effect=none; "
            "no broker/order/official target weights."
        ),
        "next_action": _next_action(status),
        "active_task_count": summary.get("active_task_count", ""),
        "completed_task_count": summary.get("completed_task_count", ""),
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_governance_artifacts_only",
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_modify_task_registers": True,
        "does_not_modify_report_registry": True,
        "does_not_modify_production": True,
        "official_target_weights": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "manual_review_only": True,
        "production_effect": PRODUCTION_EFFECT,
    }


def _next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "fix_blocking_task_register_consistency_issues_before_governance_pack"
    if status == WARN_STATUS:
        return "review_task_register_consistency_warnings_before_next_governance_cycle"
    return "continue_governance_task_sequence"


def _row_payload(row: TaskRegisterRow) -> dict[str, Any]:
    return {
        "task_id": row.task_id,
        "base_task_id": row.base_task_id,
        "domain": row.domain,
        "priority": row.priority,
        "status": row.status,
        "next_owner": row.next_owner,
        "source": row.source,
        "line_number": row.line_number,
        "docs_links": list(row.docs_links),
    }


def _source_artifact(artifact_id: str, path: Path) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": path.exists(),
        "production_effect": PRODUCTION_EFFECT,
    }


def _base_task_id(task_id: str) -> str:
    match = re.match(r"^([A-Z]+-\d+)", task_id)
    return match.group(1) if match else task_id


def _docs_links(text: str) -> list[str]:
    links = []
    for match in re.findall(r"docs/[A-Za-z0-9_./-]+\.md", text):
        links.append(match.rstrip("`.,);]"))
    return list(dict.fromkeys(links))


def _duplicate_values(values: Sequence[str]) -> list[str]:
    return sorted(value for value in set(values) if value and values.count(value) > 1)


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


def _issue_row(issue: Mapping[str, Any]) -> str:
    return (
        f"|{_markdown_cell(issue.get('issue_id'))}|"
        f"{_markdown_cell(issue.get('scope'))}|"
        f"{_markdown_cell(issue.get('task_id'))}|"
        f"{_markdown_cell(issue.get('message'))}|"
        f"{_markdown_cell(issue.get('recommended_action'))}|"
    )
