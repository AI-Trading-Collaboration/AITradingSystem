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
REPORT_TYPE = "research_governance_end_to_end_pack"
VALIDATION_REPORT_TYPE = "research_governance_end_to_end_pack_validation"

GOVERNANCE_HEALTHY = "GOVERNANCE_HEALTHY"
GOVERNANCE_HEALTHY_WITH_WARNINGS = "GOVERNANCE_HEALTHY_WITH_WARNINGS"
GOVERNANCE_MANUAL_REVIEW_REQUIRED = "GOVERNANCE_MANUAL_REVIEW_REQUIRED"
GOVERNANCE_BLOCKED = "GOVERNANCE_BLOCKED"
GOVERNANCE_STATUSES = (
    GOVERNANCE_HEALTHY,
    GOVERNANCE_HEALTHY_WITH_WARNINGS,
    GOVERNANCE_MANUAL_REVIEW_REQUIRED,
    GOVERNANCE_BLOCKED,
)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

REQUIRED_SECTIONS = (
    "source_reports",
    "overall_governance_status",
    "top_blockers",
    "warning_items",
    "manual_review_items",
    "next_actions",
    "reader_brief",
    "safety_boundary",
)

SOURCE_REPORT_SPECS = (
    {
        "source_id": "task_register_consistency",
        "report_id": "task_register_consistency",
        "validation_report_id": "task_register_consistency_validation",
        "label": "Task Register Consistency",
    },
    {
        "source_id": "registry_warning_waiver_expiry",
        "report_id": "report_index_waiver_inventory",
        "validation_report_id": "report_index_waiver_inventory_validation",
        "label": "Registry Warning Waiver Expiry",
    },
    {
        "source_id": "reader_brief_consistency_pack",
        "report_id": "reader_brief_consistency_pack",
        "validation_report_id": "reader_brief_consistency_validation",
        "label": "Reader Brief Consistency Pack",
    },
    {
        "source_id": "research_safety_boundary_audit",
        "report_id": "research_safety_boundary_audit",
        "validation_report_id": "research_safety_boundary_validation",
        "label": "Research Safety Boundary Audit",
    },
    {
        "source_id": "production_boundary_static_scan",
        "report_id": "production_boundary_static_scan",
        "validation_report_id": "production_boundary_static_scan_validation",
        "label": "Production Boundary Static Scanner",
    },
    {
        "source_id": "owner_review_template_v2",
        "report_id": "owner_review_template_v2",
        "validation_report_id": "owner_review_template_v2_validation",
        "label": "Owner Review Template V2",
    },
    {
        "source_id": "owner_decision_audit_log",
        "report_id": "owner_decision_audit_log",
        "validation_report_id": "owner_decision_audit_log_validation",
        "label": "Owner Decision Audit Log",
    },
    {
        "source_id": "research_monthly_review_pack",
        "report_id": "research_monthly_review_pack",
        "validation_report_id": "research_monthly_review_pack_validation",
        "label": "Research Monthly Review Pack",
    },
    {
        "source_id": "paper_shadow_promotion_board",
        "report_id": "paper_shadow_promotion_board",
        "validation_report_id": "paper_shadow_promotion_board_validation",
        "label": "Paper Shadow Promotion Board",
    },
    {
        "source_id": "extended_shadow_protocol",
        "report_id": "extended_shadow_protocol",
        "validation_report_id": "extended_shadow_protocol_validation",
        "label": "Extended Shadow Protocol",
    },
    {
        "source_id": "research_roadmap_dashboard",
        "report_id": "research_roadmap_dashboard",
        "validation_report_id": "research_roadmap_dashboard_validation",
        "label": "Research Roadmap Dashboard",
    },
    {
        "source_id": "artifact_lineage_graph",
        "report_id": "artifact_lineage_graph",
        "validation_report_id": "artifact_lineage_validation",
        "label": "Artifact Lineage Graph",
    },
)


def default_research_governance_end_to_end_pack_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_governance_end_to_end_pack_{as_of.isoformat()}.json"


def default_research_governance_end_to_end_pack_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_governance_end_to_end_pack_{as_of.isoformat()}.md"


def default_research_governance_end_to_end_pack_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return (
        output_dir
        / f"research_governance_end_to_end_pack_validation_{as_of.isoformat()}.json"
    )


def default_research_governance_end_to_end_pack_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return (
        output_dir
        / f"research_governance_end_to_end_pack_validation_{as_of.isoformat()}.md"
    )


def latest_research_governance_end_to_end_pack_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "research_governance_end_to_end_pack_", ".json")


def build_research_governance_end_to_end_pack_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    if report_index_payload is None:
        source_path = report_index_path or (
            project_root / "outputs" / "reports" / f"report_index_{as_of.isoformat()}.json"
        )
        report_index_payload = _read_json_mapping(source_path)
        report_index_path = source_path

    source_reports = _source_reports(report_index_payload, project_root=project_root)
    top_blockers = _top_blockers(source_reports, report_index_payload)
    warning_items = _warning_items(source_reports, report_index_payload)
    manual_review_items = _manual_review_items(source_reports)
    next_actions = _next_actions(top_blockers, manual_review_items, warning_items)
    overall_status = _overall_status(top_blockers, manual_review_items, warning_items)
    source_summary = _source_summary(source_reports)
    report_index_summary = _report_index_summary(report_index_payload)
    summary = {
        "overall_governance_status": overall_status,
        "source_report_count": source_summary["source_report_count"],
        "available_source_count": source_summary["available_source_count"],
        "validation_pass_count": source_summary["validation_pass_count"],
        "validation_warning_count": source_summary["validation_warning_count"],
        "validation_fail_count": source_summary["validation_fail_count"],
        "blocking_item_count": len(top_blockers),
        "warning_item_count": len(warning_items),
        "manual_review_item_count": len(manual_review_items),
        "top_blocker": top_blockers[0]["source_id"] if top_blockers else "none",
        "next_action": next_actions[0]["recommended_action"] if next_actions else "none",
        "report_index_status": report_index_summary["status"],
        "production_effect": PRODUCTION_EFFECT,
    }
    reader_brief = _reader_brief(summary, top_blockers, warning_items)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": overall_status,
        "overall_governance_status": overall_status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "purpose": (
            "Provide one read-only end-to-end governance pack across task register, "
            "waiver, Reader Brief, safety, owner, paper-shadow, roadmap, and lineage "
            "artifacts."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            **{
                source["report_id"]: source.get("artifact_path", "")
                for source in source_reports
            },
            **{
                source["validation_report_id"]: source.get("validation_artifact_path", "")
                for source in source_reports
            },
        },
        "output_decision": overall_status,
        "summary": summary,
        "report_index_summary": report_index_summary,
        "source_summary": source_summary,
        "source_reports": source_reports,
        "overall_governance_status_detail": {
            "status": overall_status,
            "status_policy": "TRADING-384 governance end-to-end status rules",
            "blocking_item_count": len(top_blockers),
            "warning_item_count": len(warning_items),
            "manual_review_item_count": len(manual_review_items),
        },
        "top_blockers": top_blockers,
        "warning_items": warning_items,
        "manual_review_items": manual_review_items,
        "next_actions": next_actions,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "End-to-end pack is read-only and does not run upstream reports.",
            "Owner decision, data, evidence, and metric gaps remain visible blockers.",
            "Pack status is manual governance evidence, not trading approval.",
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_report_index_and_latest_governance_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_task_register": True,
            "does_not_modify_strategy_logic": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_research_governance_end_to_end_pack_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    source_reports = _records(payload.get("source_reports"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_research_governance_end_to_end_pack",
    )
    _append_check(
        checks,
        blocking_issues,
        "governance_status_enum",
        _text(payload.get("overall_governance_status")) in GOVERNANCE_STATUSES,
        "overall_governance_status must use the supported governance enum.",
        "restore_supported_governance_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "End-to-end governance pack must be production_effect=none.",
        "restore_governance_pack_safety_boundary",
    )
    missing_sections = [
        section for section in REQUIRED_SECTIONS if not _section_present(payload, section)
    ]
    _append_check(
        checks,
        blocking_issues,
        "required_sections_present",
        not missing_sections,
        "End-to-end governance pack must include every required section.",
        "regenerate_pack_with_required_sections",
        details={"missing_sections": missing_sections},
    )
    missing_sources = [
        _text(source.get("source_id"))
        for source in source_reports
        if _text(source.get("availability")) != "AVAILABLE"
        or _text(source.get("validation_availability")) != "AVAILABLE"
    ]
    _append_check(
        checks,
        blocking_issues,
        "required_sources_available",
        not missing_sources and len(source_reports) == len(SOURCE_REPORT_SPECS),
        "All required governance source reports and validations must be available.",
        "run_missing_governance_reports_before_end_to_end_pack",
        details={"missing_sources": missing_sources},
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("task_register_mutated") is False
            and safety.get("strategy_logic_mutated") is False
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("production_state_mutated") is False
            and safety.get("official_target_weights_generated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
        ),
        (
            "End-to-end governance pack must not mutate task, strategy, candidate, "
            "shadow, production, broker, or order state."
        ),
        "restore_governance_pack_safety_boundary",
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
        "restore_governance_pack_reader_brief_fields",
    )
    if _text(summary.get("overall_governance_status")) != GOVERNANCE_HEALTHY:
        warning_issues.append(
            {
                "issue_id": "governance_pack_contains_visible_limitations",
                "overall_governance_status": _text(summary.get("overall_governance_status")),
                "message": "Pack is structurally valid but has visible governance limitations.",
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
        "source_report_count": _int(summary.get("source_report_count")),
        "source_blocker_count": _int(summary.get("blocking_item_count")),
        "source_warning_count": _int(summary.get("warning_item_count")),
        "manual_review_item_count": _int(summary.get("manual_review_item_count")),
        "overall_governance_status": _text(payload.get("overall_governance_status")),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_governance_status": _text(payload.get("overall_governance_status")),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "reader_brief": {
            "summary": (
                f"Research governance end-to-end validation is {validation_status}; "
                f"failed_checks={validation_summary['failed_check_count']}."
            ),
            "key_result": validation_status,
            "blocking_issues": (
                "none"
                if not blocking_issues
                else "; ".join(_text(issue.get("issue_id")) for issue in blocking_issues[:5])
            ),
            "warnings": (
                "none"
                if not warning_issues
                else "; ".join(_text(issue.get("issue_id")) for issue in warning_issues[:5])
            ),
            "safety_boundary": "read-only governance validation; production_effect=none",
            "next_action": (
                "fix_validation_failures"
                if validation_status == FAIL_STATUS
                else "review_end_to_end_governance_pack_findings"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _safety_boundary(),
    }


def write_research_governance_end_to_end_pack_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_research_governance_end_to_end_pack_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_research_governance_end_to_end_pack_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def write_research_governance_end_to_end_pack_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_research_governance_end_to_end_pack_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_research_governance_end_to_end_pack_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_research_governance_end_to_end_pack_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Research Governance End-to-End Pack - {_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- overall_governance_status: {payload.get('overall_governance_status')}",
        f"- source_reports: {summary.get('source_report_count')}",
        f"- available_sources: {summary.get('available_source_count')}",
        f"- blockers: {summary.get('blocking_item_count')}",
        f"- warnings: {summary.get('warning_item_count')}",
        f"- manual_review_items: {summary.get('manual_review_item_count')}",
        f"- top_blocker: {summary.get('top_blocker')}",
        f"- next_action: {summary.get('next_action')}",
        f"- production_effect: {payload.get('production_effect')}",
        "",
        "## Source Reports",
        "",
        "|source_id|status|validation_status|availability|artifact|validation_artifact|",
        "|---|---|---|---|---|---|",
    ]
    for source in _records(payload.get("source_reports")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    source.get("source_id"),
                    source.get("status"),
                    source.get("validation_status"),
                    source.get("availability"),
                    source.get("artifact_path"),
                    source.get("validation_artifact_path"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Top Blockers", ""])
    if not _records(payload.get("top_blockers")):
        lines.append("- none")
    for blocker in _records(payload.get("top_blockers")):
        lines.append(
            f"- {blocker.get('source_id')}: {blocker.get('message')} "
            f"-> {blocker.get('recommended_action')}"
        )
    lines.extend(["", "## Next Actions", ""])
    if not _records(payload.get("next_actions")):
        lines.append("- none")
    for action in _records(payload.get("next_actions")):
        lines.append(
            f"- {action.get('source_id')}: {action.get('recommended_action')} "
            f"({action.get('priority')})"
        )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            f"- production_effect: {payload.get('production_effect')}",
            "- no upstream rerun, no data refresh, no task/candidate/paper-shadow/"
            "production mutation, no official target, no broker, no order ticket.",
            "",
        ]
    )
    return "\n".join(lines)


def render_research_governance_end_to_end_pack_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        (
            "# Research Governance End-to-End Pack Validation - "
            f"{_text(payload.get('as_of'), 'UNKNOWN')}"
        ),
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_governance_status: {payload.get('source_governance_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed_checks: {summary.get('failed_check_count')}",
        f"- source_blockers: {summary.get('source_blocker_count')}",
        f"- source_warnings: {summary.get('source_warning_count')}",
        f"- production_effect: {payload.get('production_effect')}",
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


def _source_reports(
    report_index: Mapping[str, Any],
    *,
    project_root: Path,
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for spec in SOURCE_REPORT_SPECS:
        report_id = _text(spec.get("report_id"))
        validation_report_id = _text(spec.get("validation_report_id"))
        entry = _report_index_entry(report_index, report_id)
        validation_entry = _report_index_entry(report_index, validation_report_id)
        artifact_path = _resolve_artifact_path(
            _text(entry.get("latest_artifact_path")),
            project_root,
        )
        validation_artifact_path = _resolve_artifact_path(
            _text(validation_entry.get("latest_artifact_path")),
            project_root,
        )
        payload_path, payload = _read_context_payload(artifact_path)
        validation_payload_path, validation_payload = _read_context_payload(
            validation_artifact_path
        )
        summary = _mapping(payload.get("summary"))
        validation_summary = _mapping(validation_payload.get("summary"))
        reports.append(
            {
                "source_id": _text(spec.get("source_id")),
                "label": _text(spec.get("label")),
                "report_id": report_id,
                "validation_report_id": validation_report_id,
                "availability": (
                    "AVAILABLE"
                    if artifact_path is not None and artifact_path.exists()
                    else "MISSING"
                ),
                "validation_availability": (
                    "AVAILABLE"
                    if validation_artifact_path is not None
                    and validation_artifact_path.exists()
                    else "MISSING"
                ),
                "artifact_path": "" if artifact_path is None else str(artifact_path),
                "validation_artifact_path": (
                    "" if validation_artifact_path is None else str(validation_artifact_path)
                ),
                "source_payload_path": "" if payload_path is None else str(payload_path),
                "validation_payload_path": (
                    ""
                    if validation_payload_path is None
                    else str(validation_payload_path)
                ),
                "status": _source_status(payload, entry),
                "validation_status": _source_status(validation_payload, validation_entry),
                "summary": dict(summary),
                "validation_summary": dict(validation_summary),
                "next_action": _text(payload.get("next_action")),
                "production_effect": _text(
                    payload.get("production_effect"),
                    _text(entry.get("production_effect"), PRODUCTION_EFFECT),
                ),
                "validation_production_effect": _text(
                    validation_payload.get("production_effect"),
                    _text(validation_entry.get("production_effect"), PRODUCTION_EFFECT),
                ),
            }
        )
    return reports


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


def _source_summary(source_reports: list[Mapping[str, Any]]) -> dict[str, Any]:
    validation_statuses = [_text(source.get("validation_status")) for source in source_reports]
    return {
        "source_report_count": len(source_reports),
        "available_source_count": len(
            [
                source
                for source in source_reports
                if _text(source.get("availability")) == "AVAILABLE"
            ]
        ),
        "validation_pass_count": len(
            [status for status in validation_statuses if status in {"PASS", "OK"}]
        ),
        "validation_warning_count": len(
            [
                status
                for status in validation_statuses
                if status in {"WARNING", "PASS_WITH_WARNINGS", "SAFETY_PASS_WITH_WARNINGS"}
            ]
        ),
        "validation_fail_count": len(
            [
                status
                for status in validation_statuses
                if "FAIL" in status or "BLOCK" in status
            ]
        ),
    }


def _report_index_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(report_index.get("summary"))
    return {
        "status": _text(report_index.get("status"), "UNKNOWN"),
        "report_count": _int(summary.get("report_count")),
        "missing_count": _int(summary.get("missing_count")),
        "stale_count": _int(summary.get("stale_count")),
        "required_missing_count": _int(summary.get("required_missing_count")),
        "unwaived_warning_count": _int(summary.get("unwaived_warning_count")),
        "explicit_waiver_count": _int(summary.get("explicit_waiver_count")),
    }


def _top_blockers(
    source_reports: list[Mapping[str, Any]],
    report_index: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    report_index_summary = _report_index_summary(report_index)
    if _int(report_index_summary.get("unwaived_warning_count")) > 0:
        blockers.append(
            _issue(
                "report_index_unwaived_warnings",
                "report_index",
                "BLOCKING",
                "Report index has unwaived missing or stale warnings.",
                "repair_or_explicitly_waive_report_index_findings",
            )
        )
    for source in source_reports:
        source_id = _text(source.get("source_id"))
        status = _text(source.get("status"))
        validation_status = _text(source.get("validation_status"))
        if _text(source.get("availability")) != "AVAILABLE":
            blockers.append(
                _issue(
                    f"{source_id}_missing",
                    source_id,
                    "BLOCKING",
                    "Required source report is missing.",
                    "run_missing_governance_report",
                )
            )
            continue
        if _text(source.get("validation_availability")) != "AVAILABLE":
            blockers.append(
                _issue(
                    f"{source_id}_validation_missing",
                    source_id,
                    "BLOCKING",
                    "Required source validation report is missing.",
                    "run_missing_governance_validation",
                )
            )
        if _is_blocking_status(status) or _is_blocking_status(validation_status):
            message = (
                f"Required source is blocking: status={status}; "
                f"validation={validation_status}."
            )
            blockers.append(
                _issue(
                    f"{source_id}_blocked",
                    source_id,
                    "BLOCKING",
                    message,
                    _text(source.get("next_action"), "review_source_blockers"),
                )
            )
    return blockers[:20]


def _warning_items(
    source_reports: list[Mapping[str, Any]],
    report_index: Mapping[str, Any],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    report_index_summary = _report_index_summary(report_index)
    if _int(report_index_summary.get("stale_count")) > 0:
        warnings.append(
            _issue(
                "report_index_stale_artifacts",
                "report_index",
                "WARNING",
                "Report index has stale artifacts, including waived issues.",
                "review_stale_artifact_waivers_and_refresh_plan",
            )
        )
    for source in source_reports:
        source_id = _text(source.get("source_id"))
        status = _text(source.get("status"))
        validation_status = _text(source.get("validation_status"))
        if _is_warning_status(status) or _is_warning_status(validation_status):
            message = (
                f"Required source has warning status: {status}; "
                f"validation={validation_status}."
            )
            warnings.append(
                _issue(
                    f"{source_id}_warning",
                    source_id,
                    "WARNING",
                    message,
                    _text(source.get("next_action"), "review_source_warnings"),
                )
            )
    return warnings[:20]


def _manual_review_items(source_reports: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    review_items: list[dict[str, Any]] = []
    for source in source_reports:
        source_id = _text(source.get("source_id"))
        status = _text(source.get("status"))
        if status in {
            "AUDIT_LOG_EMPTY",
            "TEMPLATE_READY",
            "HOLD_FOR_MORE_DATA",
            "GOVERNANCE_MANUAL_REVIEW_REQUIRED",
        }:
            review_items.append(
                _issue(
                    f"{source_id}_manual_review",
                    source_id,
                    "MANUAL_REVIEW",
                    f"Required source needs manual review or owner follow-up: {status}.",
                    _text(source.get("next_action"), "complete_manual_owner_review"),
                )
            )
    return review_items[:20]


def _next_actions(
    blockers: list[Mapping[str, Any]],
    manual_review_items: list[Mapping[str, Any]],
    warning_items: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    ordered = [*blockers[:5], *manual_review_items[:3], *warning_items[:2]]
    return [
        {
            "source_id": _text(item.get("source_id")),
            "priority": _text(item.get("severity"), "P1"),
            "reason": _text(item.get("message")),
            "recommended_action": _text(item.get("recommended_action")),
        }
        for item in ordered
    ]


def _overall_status(
    blockers: list[Mapping[str, Any]],
    manual_review_items: list[Mapping[str, Any]],
    warning_items: list[Mapping[str, Any]],
) -> str:
    if blockers:
        return GOVERNANCE_BLOCKED
    if manual_review_items:
        return GOVERNANCE_MANUAL_REVIEW_REQUIRED
    if warning_items:
        return GOVERNANCE_HEALTHY_WITH_WARNINGS
    return GOVERNANCE_HEALTHY


def _reader_brief(
    summary: Mapping[str, Any],
    blockers: list[Mapping[str, Any]],
    warnings: list[Mapping[str, Any]],
) -> dict[str, Any]:
    status = _text(summary.get("overall_governance_status"), GOVERNANCE_BLOCKED)
    return {
        "summary": (
            f"Research governance end-to-end pack is {status}; sources="
            f"{_int(summary.get('source_report_count'))}, blockers="
            f"{_int(summary.get('blocking_item_count'))}."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if not blockers
            else "; ".join(_text(blocker.get("source_id")) for blocker in blockers[:5])
        ),
        "warnings": (
            "none"
            if not warnings
            else "; ".join(_text(warning.get("source_id")) for warning in warnings[:5])
        ),
        "safety_boundary": "read-only governance pack; production_effect=none",
        "next_action": _text(summary.get("next_action"), "review_governance_pack_findings"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _source_status(payload: Mapping[str, Any], entry: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    for field in (
        "validation_status",
        "overall_governance_status",
        "dashboard_status",
        "eligibility_status",
        "board_decision",
        "monthly_review_status",
        "audit_log_status",
        "template_status",
        "scan_status",
        "safety_status",
        "lineage_status",
        "consistency_status",
        "waiver_inventory_status",
        "status",
    ):
        value = _text(payload.get(field), _text(summary.get(field)))
        if value:
            return value
    for field in ("status", "freshness_status", "artifact_status"):
        value = _text(entry.get(field))
        if value:
            return value
    return "MISSING" if not entry else "UNKNOWN"


def _is_blocking_status(status: str) -> bool:
    normalized = status.upper()
    if normalized in {"MISSING", "FAIL", "FAILED", "BLOCKING", "SAFETY_BLOCKED"}:
        return True
    return any(
        token in normalized
        for token in (
            "_BLOCKED",
            "BLOCKED_",
            "MONTHLY_REVIEW_BLOCKED",
            "ROADMAP_BLOCKED",
            "GOVERNANCE_BLOCKED",
            "EXTENDED_SHADOW_BLOCKED",
            "HOLD_FOR_MORE_DATA",
            "RETURN_TO_RESEARCH",
            "REJECT",
        )
    )


def _is_warning_status(status: str) -> bool:
    normalized = status.upper()
    return "WARNING" in normalized or normalized in {
        "WARN",
        "PASS_WITH_WARNINGS",
        "SAFETY_PASS_WITH_WARNINGS",
    }


def _issue(
    issue_id: str,
    source_id: str,
    severity: str,
    message: str,
    recommended_action: str,
) -> dict[str, Any]:
    return {
        "issue_id": issue_id,
        "source_id": source_id,
        "severity": severity,
        "message": message,
        "recommended_action": recommended_action,
    }


def _section_present(payload: Mapping[str, Any], section: str) -> bool:
    value = payload.get(section)
    if isinstance(value, (dict, list)):
        return True
    return bool(_text(value))


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_report_index_and_latest_governance_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "task_register_mutated": False,
        "strategy_logic_mutated": False,
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
