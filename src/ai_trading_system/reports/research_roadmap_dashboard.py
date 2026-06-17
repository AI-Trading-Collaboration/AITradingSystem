from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _int,
    _latest_dated_path,
    _mapping,
    _md_cell,
    _read_json_mapping,
    _read_optional_json_mapping,
    _records,
    _report_index_entry,
    _resolve_artifact_path,
    _text,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "research_roadmap_dashboard"
VALIDATION_REPORT_TYPE = "research_roadmap_dashboard_validation"

ROADMAP_HEALTHY = "ROADMAP_HEALTHY"
ROADMAP_WARNINGS = "ROADMAP_WARNINGS"
ROADMAP_WITH_WARNINGS = "ROADMAP_WITH_WARNINGS"
ROADMAP_BLOCKED = "ROADMAP_BLOCKED"
ROADMAP_STATUSES = (
    ROADMAP_HEALTHY,
    ROADMAP_WARNINGS,
    ROADMAP_WITH_WARNINGS,
    ROADMAP_BLOCKED,
)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

REQUIRED_SECTIONS = (
    "active_task_summary",
    "completed_task_summary",
    "open_blockers",
    "stale_artifacts",
    "active_candidates",
    "latest_paper_shadow_status",
    "latest_data_governance",
    "latest_safety",
    "latest_lineage",
    "next_recommended_tasks",
)

CONTEXT_REPORT_IDS = (
    "research_monthly_review_pack",
    "paper_shadow_promotion_board",
    "extended_shadow_protocol",
    "candidate_rejection_postmortem_template",
    "research_safety_boundary_audit",
    "artifact_lineage_graph",
    "task_register_consistency",
)


def default_research_roadmap_dashboard_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_roadmap_dashboard_{as_of.isoformat()}.json"


def default_research_roadmap_dashboard_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_roadmap_dashboard_{as_of.isoformat()}.md"


def default_research_roadmap_dashboard_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_roadmap_dashboard_validation_{as_of.isoformat()}.json"


def default_research_roadmap_dashboard_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_roadmap_dashboard_validation_{as_of.isoformat()}.md"


def latest_research_roadmap_dashboard_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "research_roadmap_dashboard_", ".json")


def build_research_roadmap_dashboard_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    task_register_path: Path = PROJECT_ROOT / "docs" / "task_register.md",
    completed_register_path: Path = PROJECT_ROOT / "docs" / "task_register_completed.md",
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    if report_index_payload is None:
        source_path = report_index_path or (
            project_root / "outputs" / "reports" / f"report_index_{as_of.isoformat()}.json"
        )
        report_index_payload = _read_json_mapping(source_path)
        report_index_path = source_path

    active_tasks = _task_rows(task_register_path)
    completed_tasks = _task_rows(completed_register_path)
    context = _context_reports(report_index_payload, project_root=project_root)
    report_index_summary = _mapping(report_index_payload.get("summary"))
    active_task_summary = _active_task_summary(active_tasks)
    completed_task_summary = _completed_task_summary(completed_tasks)
    stale_artifacts = _stale_artifact_summary(report_index_payload, report_index_summary)
    active_candidates = _active_candidate_summary(context)
    paper_shadow_status = _paper_shadow_status(context)
    data_governance = _data_governance_status(context, report_index_summary)
    safety = _safety_status(context)
    lineage = _lineage_status(context)
    open_blockers = _open_blockers(
        active_task_summary=active_task_summary,
        stale_artifacts=stale_artifacts,
        active_candidates=active_candidates,
        paper_shadow_status=paper_shadow_status,
        data_governance=data_governance,
        safety=safety,
        lineage=lineage,
    )
    next_tasks = _next_recommended_tasks(open_blockers, active_tasks)
    dashboard_status = _dashboard_status(
        open_blockers,
        stale_artifacts,
        paper_shadow_status,
        safety,
        lineage,
    )
    summary = {
        "dashboard_status": dashboard_status,
        "active_task_count": active_task_summary["active_task_count"],
        "completed_task_count": completed_task_summary["completed_task_count"],
        "open_blocker_count": len(open_blockers),
        "stale_artifact_count": stale_artifacts["stale_count"],
        "missing_artifact_count": stale_artifacts["missing_count"],
        "active_candidate_count": active_candidates["active_candidate_count"],
        "paper_shadow_status": paper_shadow_status["status"],
        "data_governance_status": data_governance["status"],
        "safety_status": safety["status"],
        "lineage_status": lineage["status"],
        "top_next_task": next_tasks[0]["task_id"] if next_tasks else "none",
        "production_effect": PRODUCTION_EFFECT,
    }
    reader_brief = _reader_brief(summary, open_blockers)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": dashboard_status,
        "dashboard_status": dashboard_status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "purpose": (
            "Aggregate research roadmap tasks, blockers, stale artifacts, candidates, "
            "paper-shadow, data governance, safety, lineage, and next recommended tasks."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            "task_register": str(task_register_path),
            "completed_task_register": str(completed_register_path),
            **{key: value.get("artifact_path", "") for key, value in context.items()},
        },
        "output_decision": dashboard_status,
        "summary": summary,
        "required_sections": list(REQUIRED_SECTIONS),
        "active_task_summary": active_task_summary,
        "completed_task_summary": completed_task_summary,
        "open_blockers": open_blockers,
        "stale_artifacts": stale_artifacts,
        "active_candidates": active_candidates,
        "latest_paper_shadow_status": paper_shadow_status,
        "latest_data_governance": data_governance,
        "latest_safety": safety,
        "latest_lineage": lineage,
        "next_recommended_tasks": next_tasks,
        "source_context": context,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Roadmap dashboard is read-only and does not change task states.",
            "Recommended tasks are manual review suggestions, not automatic scheduling.",
            "Missing or stale artifacts remain visible and are not repaired by this report.",
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_task_registers_report_index_and_latest_governance_artifacts",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_task_register": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_research_roadmap_dashboard_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_research_roadmap_dashboard",
    )
    _append_check(
        checks,
        blocking_issues,
        "dashboard_status_enum",
        _text(payload.get("dashboard_status")) in ROADMAP_STATUSES,
        "dashboard_status must use the supported roadmap enum.",
        "restore_supported_roadmap_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "Roadmap dashboard must be production_effect=none.",
        "restore_roadmap_safety_boundary",
    )
    missing_sections = [
        section for section in REQUIRED_SECTIONS if not _section_present(payload, section)
    ]
    _append_check(
        checks,
        blocking_issues,
        "required_sections_present",
        not missing_sections,
        "Roadmap dashboard must include every required section.",
        "regenerate_roadmap_with_required_sections",
        details={"missing_sections": missing_sections},
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("task_register_mutated") is False
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("production_state_mutated") is False
            and safety.get("official_target_weights_generated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
        ),
        (
            "Roadmap dashboard must not mutate tasks, candidate, shadow, production, "
            "broker, or order state."
        ),
        "restore_roadmap_safety_boundary",
    )
    reader_brief = _mapping(payload.get("reader_brief"))
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        all(
            bool(_text(reader_brief.get(field)))
            for field in (
                "summary",
                "key_result",
                "blocking_issues",
                "warnings",
                "safety_boundary",
                "next_action",
            )
        ),
        (
            "Reader Brief section must expose summary, key result, blockers, warnings, "
            "safety, and next action."
        ),
        "restore_roadmap_reader_brief_fields",
    )
    if _text(summary.get("dashboard_status")) != ROADMAP_HEALTHY:
        warning_issues.append(
            {
                "issue_id": "roadmap_contains_visible_work_or_artifact_limitations",
                "dashboard_status": _text(summary.get("dashboard_status")),
                "message": (
                    "Roadmap is structurally valid but contains visible work or "
                    "artifact limitations."
                ),
                "recommended_action": _text(payload.get("next_action")),
            }
        )

    validation_status = FAIL_STATUS
    if not blocking_issues:
        validation_status = PASS_WITH_WARNINGS_STATUS if warning_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "warning_check_count": len(warning_issues),
        "blocking_issue_count": len(blocking_issues),
        "dashboard_status": _text(payload.get("dashboard_status")),
        "open_blocker_count": _int(summary.get("open_blocker_count")),
        "source_blocker_count": _int(summary.get("open_blocker_count")),
        "source_warning_count": len(warning_issues),
        "active_task_count": _int(summary.get("active_task_count")),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_dashboard_status": _text(payload.get("dashboard_status"), "UNKNOWN"),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "purpose": "Validate research roadmap dashboard schema, sections, and safety boundary.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": validation_status,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "source_summary": dict(summary),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation does not modify task register or repair artifacts.",
            "PASS_WITH_WARNINGS means roadmap limitations are visible for manual review.",
        ],
        "next_action": (
            "use_roadmap_dashboard_for_manual_prioritization"
            if validation_status != FAIL_STATUS
            else "repair_roadmap_dashboard_schema_or_safety"
        ),
        "reader_brief": _reader_brief(summary, _records(payload.get("open_blockers"))),
        "methodology": {
            "collector_mode": "validate_existing_research_roadmap_dashboard_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_task_register": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_research_roadmap_dashboard_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_research_roadmap_dashboard_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_research_roadmap_dashboard_markdown(payload), encoding="utf-8")
    return output_path


def write_research_roadmap_dashboard_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_research_roadmap_dashboard_json(payload, output_path)


def write_research_roadmap_dashboard_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_research_roadmap_dashboard_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_research_roadmap_dashboard_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Research Roadmap Dashboard {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- dashboard_status: {payload.get('dashboard_status')}",
        f"- active_tasks: {summary.get('active_task_count')}",
        f"- completed_tasks: {summary.get('completed_task_count')}",
        f"- open_blockers: {summary.get('open_blocker_count')}",
        f"- stale_artifacts: {summary.get('stale_artifact_count')}",
        f"- active_candidates: {summary.get('active_candidate_count')}",
        f"- paper_shadow_status: {summary.get('paper_shadow_status')}",
        f"- data_governance_status: {summary.get('data_governance_status')}",
        f"- safety_status: {summary.get('safety_status')}",
        f"- lineage_status: {summary.get('lineage_status')}",
        f"- top_next_task: {summary.get('top_next_task')}",
        f"- production_effect: {payload.get('production_effect')}",
        "",
        "## Open Blockers",
        "",
        "|blocker_id|source|severity|message|recommended_action|",
        "|---|---|---|---|---|",
    ]
    for blocker in _records(payload.get("open_blockers")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    blocker.get("blocker_id"),
                    blocker.get("source"),
                    blocker.get("severity"),
                    blocker.get("message"),
                    blocker.get("recommended_action"),
                )
            )
            + "|"
        )
    lines.extend(
        [
            "",
            "## Next Recommended Tasks",
            "",
            "|task_id|priority|reason|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for task in _records(payload.get("next_recommended_tasks")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    task.get("task_id"),
                    task.get("priority"),
                    task.get("reason"),
                    task.get("recommended_action"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def render_research_roadmap_dashboard_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Research Roadmap Dashboard Validation {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_dashboard_status: {payload.get('source_dashboard_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        "",
        "## Checks",
        "",
        "|check_id|status|message|recommended_action|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    check.get("check_id"),
                    check.get("status"),
                    check.get("message"),
                    check.get("recommended_action"),
                )
            )
            + "|"
        )
    lines.append("")
    return "\n".join(lines)


def _task_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|") or line.startswith("|---"):
            continue
        parts = line.split("|", 5)
        if len(parts) < 6:
            continue
        task_id = parts[1].strip()
        if task_id.lower() in {"id", "task id"} or set(task_id) <= {"-"}:
            continue
        rows.append(
            {
                "task_id": task_id,
                "domain": parts[2].strip(),
                "priority": parts[3].strip(),
                "status": parts[4].strip(),
                "row_tail": parts[5].strip(),
            }
        )
    return rows


def _context_reports(
    report_index: Mapping[str, Any],
    *,
    project_root: Path,
) -> dict[str, dict[str, Any]]:
    context: dict[str, dict[str, Any]] = {}
    for report_id in CONTEXT_REPORT_IDS:
        entry = _report_index_entry(report_index, report_id)
        artifact_path = _resolve_artifact_path(
            _text(entry.get("latest_artifact_path")),
            project_root,
        )
        payload_path, payload = _read_context_payload(artifact_path)
        context[report_id] = {
            "report_id": report_id,
            "availability": (
                "AVAILABLE"
                if artifact_path is not None and artifact_path.exists()
                else "MISSING"
            ),
            "artifact_path": "" if artifact_path is None else str(artifact_path),
            "source_payload_path": "" if payload_path is None else str(payload_path),
            "status": _source_status(payload, entry),
            "summary": dict(_mapping(payload.get("summary"))),
            "production_effect": _text(
                payload.get("production_effect"),
                _text(entry.get("production_effect"), PRODUCTION_EFFECT),
            ),
        }
    return context


def _read_context_payload(artifact_path: Path | None) -> tuple[Path | None, dict[str, Any]]:
    if artifact_path is None:
        return None, {}
    candidates = [artifact_path, artifact_path.with_suffix(".json")]
    seen: set[str] = set()
    for candidate in candidates:
        if str(candidate) in seen:
            continue
        seen.add(str(candidate))
        payload = _read_optional_json_mapping(candidate)
        if payload:
            return candidate, payload
    return None, {}


def _active_task_summary(tasks: list[Mapping[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    priority_counts: dict[str, int] = {}
    for task in tasks:
        status = _text(task.get("status"), "UNKNOWN")
        priority = _text(task.get("priority"), "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
    blocked = [task for task in tasks if _text(task.get("status")).startswith("BLOCKED")]
    return {
        "active_task_count": len(tasks),
        "status_counts": status_counts,
        "priority_counts": priority_counts,
        "blocked_task_count": len(blocked),
        "top_active_tasks": [dict(task) for task in tasks[:10]],
    }


def _completed_task_summary(tasks: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "completed_task_count": len(tasks),
        "latest_completed_tasks": [dict(task) for task in tasks[:10]],
    }


def _stale_artifact_summary(
    report_index: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    problem_reports = [
        {
            "report_id": _text(report.get("report_id")),
            "freshness_status": _text(report.get("freshness_status")),
            "artifact_status": _text(report.get("artifact_status")),
            "owner_action": _text(report.get("owner_action")),
        }
        for report in _records(report_index.get("reports"))
        if _text(report.get("freshness_status")).upper() == "STALE"
        or _text(report.get("artifact_status")).upper() == "MISSING"
    ][:20]
    return {
        "report_index_status": _text(report_index.get("status"), "UNKNOWN"),
        "report_count": _int(summary.get("report_count")),
        "missing_count": _int(summary.get("missing_count")),
        "stale_count": _int(summary.get("stale_count")),
        "required_missing_count": _int(summary.get("required_missing_count")),
        "unwaived_warning_count": _int(summary.get("unwaived_warning_count")),
        "explicit_waiver_count": _int(summary.get("explicit_waiver_count")),
        "problem_reports": problem_reports,
    }


def _active_candidate_summary(context: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    monthly = _mapping(context.get("research_monthly_review_pack"))
    summary = _mapping(monthly.get("summary"))
    return {
        "active_candidate_count": _int(summary.get("active_candidate_count")),
        "paper_shadow_candidate_count": _int(summary.get("paper_shadow_candidate_count")),
        "needs_evidence_candidate_count": _int(summary.get("needs_evidence_candidate_count")),
        "major_blocker_count": _int(summary.get("major_blocker_count")),
        "major_warning_count": _int(summary.get("major_warning_count")),
        "source_status": _text(monthly.get("status"), "MISSING"),
    }


def _paper_shadow_status(context: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    board = _mapping(context.get("paper_shadow_promotion_board"))
    extended = _mapping(context.get("extended_shadow_protocol"))
    board_summary = _mapping(board.get("summary"))
    extended_summary = _mapping(extended.get("summary"))
    return {
        "status": _text(board_summary.get("board_decision"), _text(board.get("status"), "MISSING")),
        "promotion_board_status": _text(board.get("status"), "MISSING"),
        "extended_shadow_status": _text(
            extended_summary.get("eligibility_status"),
            _text(extended.get("status"), "MISSING"),
        ),
        "blocked_count": _int(board_summary.get("blocked_evidence_count"))
        + _int(extended_summary.get("blocked_check_count")),
        "warning_count": _int(board_summary.get("warning_evidence_count"))
        + _int(extended_summary.get("warning_check_count")),
    }


def _data_governance_status(
    context: Mapping[str, Mapping[str, Any]],
    report_index_summary: Mapping[str, Any],
) -> dict[str, Any]:
    monthly = _mapping(context.get("research_monthly_review_pack"))
    summary = _mapping(monthly.get("summary"))
    status = _text(summary.get("data_governance_status"), "UNKNOWN")
    if status == "UNKNOWN" and _int(report_index_summary.get("unwaived_warning_count")) > 0:
        status = "BLOCKED"
    return {
        "status": status,
        "report_index_status": _text(report_index_summary.get("status")),
        "missing_count": _int(report_index_summary.get("missing_count")),
        "stale_count": _int(report_index_summary.get("stale_count")),
        "required_missing_count": _int(report_index_summary.get("required_missing_count")),
    }


def _safety_status(context: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    safety = _mapping(context.get("research_safety_boundary_audit"))
    summary = _mapping(safety.get("summary"))
    return {
        "status": _text(summary.get("safety_status"), _text(safety.get("status"), "MISSING")),
        "unsafe_signal_count": _int(summary.get("unsafe_signal_count")),
        "missing_metadata_count": _int(summary.get("missing_metadata_count")),
    }


def _lineage_status(context: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    lineage = _mapping(context.get("artifact_lineage_graph"))
    summary = _mapping(lineage.get("summary"))
    return {
        "status": _text(summary.get("lineage_status"), _text(lineage.get("status"), "MISSING")),
        "blocking_issue_count": _int(summary.get("blocking_issue_count")),
        "warning_issue_count": _int(summary.get("warning_issue_count")),
    }


def _open_blockers(
    *,
    active_task_summary: Mapping[str, Any],
    stale_artifacts: Mapping[str, Any],
    active_candidates: Mapping[str, Any],
    paper_shadow_status: Mapping[str, Any],
    data_governance: Mapping[str, Any],
    safety: Mapping[str, Any],
    lineage: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if _int(active_task_summary.get("blocked_task_count")) > 0:
        blockers.append(
            _blocker(
                "active_task_blockers",
                "task_register",
                "BLOCKING",
                "Active task register contains BLOCKED tasks.",
                "review_blocked_task_dependencies",
            )
        )
    if _int(stale_artifacts.get("unwaived_warning_count")) > 0:
        blockers.append(
            _blocker(
                "unwaived_report_index_warnings",
                "report_index",
                "BLOCKING",
                "Report index has unwaived missing or stale warnings.",
                "repair_or_explicitly_waive_report_index_findings",
            )
        )
    extended_shadow_status = _text(paper_shadow_status.get("extended_shadow_status"))
    if extended_shadow_status in {"EXTENDED_SHADOW_BLOCKED", "EXTENDED_SHADOW_NOT_READY"}:
        blocker_id = (
            "extended_shadow_not_ready"
            if extended_shadow_status == "EXTENDED_SHADOW_NOT_READY"
            else "extended_shadow_blocked"
        )
        blockers.append(
            _blocker(
                blocker_id,
                "extended_shadow_protocol",
                "BLOCKING",
                f"Extended shadow protocol is {extended_shadow_status}.",
                "resolve_extended_shadow_protocol_blockers",
            )
        )
    if _text(paper_shadow_status.get("status")) == "HOLD_FOR_MORE_DATA":
        blockers.append(
            _blocker(
                "promotion_board_hold_for_more_data",
                "paper_shadow_promotion_board",
                "BLOCKING",
                "Promotion board is holding for more evidence.",
                "resolve_promotion_board_required_evidence",
            )
        )
    if _int(active_candidates.get("major_blocker_count")) > 0:
        blockers.append(
            _blocker(
                "monthly_candidate_major_blockers",
                "research_monthly_review_pack",
                "BLOCKING",
                "Monthly review pack reports candidate or governance blockers.",
                "resolve_monthly_review_pack_blockers",
            )
        )
    if "BLOCK" in _text(data_governance.get("status")).upper():
        blockers.append(
            _blocker(
                "data_governance_blocked",
                "data_governance",
                "BLOCKING",
                "Latest data governance status is blocked.",
                "repair_data_governance_sources_before_shadow_decisions",
            )
        )
    if "SAFETY_BLOCKED" in _text(safety.get("status")).upper():
        blockers.append(
            _blocker(
                "safety_blocked",
                "research_safety_boundary_audit",
                "BLOCKING",
                "Research safety boundary audit is blocked.",
                "repair_safety_boundary_before_research_progression",
            )
        )
    if _int(lineage.get("blocking_issue_count")) > 0:
        blockers.append(
            _blocker(
                "lineage_blocked",
                "artifact_lineage_graph",
                "BLOCKING",
                "Artifact lineage graph has blocking issues.",
                "repair_lineage_required_family_or_edge_coverage",
            )
        )
    return blockers


def _next_recommended_tasks(
    blockers: list[Mapping[str, Any]],
    active_tasks: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    recommendations = [
        {
            "task_id": _text(blocker.get("blocker_id")),
            "priority": "P1",
            "reason": _text(blocker.get("message")),
            "recommended_action": _text(blocker.get("recommended_action")),
        }
        for blocker in blockers[:5]
    ]
    if not recommendations:
        recommendations.extend(
            {
                "task_id": _text(task.get("task_id")),
                "priority": _text(task.get("priority")),
                "reason": "highest_visible_active_task",
                "recommended_action": "review_task_register_acceptance_criteria",
            }
            for task in active_tasks[:5]
        )
    return recommendations


def _dashboard_status(
    blockers: list[Mapping[str, Any]],
    stale_artifacts: Mapping[str, Any],
    paper_shadow_status: Mapping[str, Any],
    safety: Mapping[str, Any],
    lineage: Mapping[str, Any],
) -> str:
    if blockers or "SAFETY_BLOCKED" in _text(safety.get("status")).upper():
        return ROADMAP_BLOCKED
    missing_count = _int(stale_artifacts.get("missing_count"))
    stale_count = _int(stale_artifacts.get("stale_count"))
    if (
        missing_count > 0
        or stale_count > 0
        or _int(paper_shadow_status.get("warning_count")) > 0
        or _int(lineage.get("warning_issue_count")) > 0
        or "WARNING" in _text(safety.get("status")).upper()
    ):
        return ROADMAP_WARNINGS
    return ROADMAP_HEALTHY


def _reader_brief(
    summary: Mapping[str, Any],
    blockers: list[Mapping[str, Any]],
) -> dict[str, Any]:
    status = _text(summary.get("dashboard_status"), ROADMAP_WARNINGS)
    return {
        "summary": (
            f"Research roadmap dashboard is {status}; active tasks="
            f"{_int(summary.get('active_task_count'))}, blockers="
            f"{_int(summary.get('open_blocker_count'))}."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if not blockers
            else "; ".join(
                f"{_text(blocker.get('source'))}:{_text(blocker.get('blocker_id'))}"
                for blocker in blockers[:5]
            )
        ),
        "warnings": (
            "report_index_missing_or_stale_visible"
            if _int(summary.get("stale_artifact_count"))
            or _int(summary.get("missing_artifact_count"))
            else "none"
        ),
        "safety_boundary": (
            "Read-only roadmap dashboard; no task register mutation, no candidate or "
            "paper-shadow mutation, no official target weights, no broker/order, "
            "production_effect=none."
        ),
        "next_action": (
            "review_top_roadmap_blockers"
            if blockers
            else "continue_reviewing_high_priority_active_tasks"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _blocker(
    blocker_id: str,
    source: str,
    severity: str,
    message: str,
    recommended_action: str,
) -> dict[str, str]:
    return {
        "blocker_id": blocker_id,
        "source": source,
        "severity": severity,
        "message": message,
        "recommended_action": recommended_action,
        "production_effect": PRODUCTION_EFFECT,
    }


def _source_status(payload: Mapping[str, Any], entry: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    for field in (
        "status",
        "dashboard_status",
        "monthly_review_status",
        "board_decision",
        "eligibility_status",
        "template_status",
        "safety_status",
        "lineage_status",
        "consistency_status",
    ):
        value = _text(payload.get(field), _text(summary.get(field)))
        if value:
            return value
    for field in ("status", "freshness_status", "artifact_status"):
        value = _text(entry.get(field))
        if value:
            return value
    return "MISSING" if not entry else "UNKNOWN"


def _section_present(payload: Mapping[str, Any], section: str) -> bool:
    value = payload.get(section)
    if isinstance(value, (dict, list)):
        return True
    return bool(_text(value))


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_task_registers_report_index_and_governance_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "task_register_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
    }


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> None:
    check = {
        "check_id": check_id,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "message": message,
        "recommended_action": recommended_action,
    }
    if details:
        check["details"] = dict(details)
    checks.append(check)
    if not passed:
        issue = {
            "issue_id": check_id,
            "message": message,
            "recommended_action": recommended_action,
        }
        if details:
            issue["details"] = dict(details)
        blocking_issues.append(issue)
