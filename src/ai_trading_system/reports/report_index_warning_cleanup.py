from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.recovery_triage import (
    latest_report_index_warning_triage_json_path,
)
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _int,
    _latest_dated_path,
    _mapping,
    _md_cell,
    _read_json_mapping,
    _records,
    _text,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "report_index_warning_cleanup"
VALIDATION_REPORT_TYPE = "report_index_warning_cleanup_validation"

REPORT_INDEX_WARNINGS_CLEARED = "REPORT_INDEX_WARNINGS_CLEARED"
REPORT_INDEX_WARNINGS_REMAIN = "REPORT_INDEX_WARNINGS_REMAIN"
REPORT_INDEX_WARNINGS_MANUAL_REVIEW_REQUIRED = (
    "REPORT_INDEX_WARNINGS_MANUAL_REVIEW_REQUIRED"
)
REPORT_INDEX_WARNING_STATUSES = (
    REPORT_INDEX_WARNINGS_CLEARED,
    REPORT_INDEX_WARNINGS_REMAIN,
    REPORT_INDEX_WARNINGS_MANUAL_REVIEW_REQUIRED,
)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

CORE_READER_BRIEF_FIELDS = (
    "summary",
    "key_result",
    "blocking_issues",
    "warnings",
    "safety_boundary",
    "next_action",
)


def default_report_index_warning_cleanup_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_warning_cleanup_{as_of.isoformat()}.json"


def default_report_index_warning_cleanup_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"report_index_warning_cleanup_{as_of.isoformat()}.md"


def default_report_index_warning_cleanup_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"report_index_warning_cleanup_validation_{as_of.isoformat()}.json"


def default_report_index_warning_cleanup_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"report_index_warning_cleanup_validation_{as_of.isoformat()}.md"


def latest_report_index_warning_cleanup_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "report_index_warning_cleanup_", ".json")


def build_report_index_warning_cleanup_payload(
    *,
    as_of: date,
    report_index_warning_triage_payload: Mapping[str, Any] | None = None,
    report_index_warning_triage_path: Path | None = None,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    warning_triage_path, warning_triage = _payload_or_latest(
        report_index_warning_triage_payload,
        report_index_warning_triage_path,
        latest_report_index_warning_triage_json_path,
        reports_dir,
        "report_index_warning_triage",
    )
    if report_index_payload is None:
        source_index = report_index_path or reports_dir / f"report_index_{as_of.isoformat()}.json"
        report_index_payload = _read_json_mapping(source_index) if source_index.exists() else {}
        report_index_path = source_index if source_index.exists() else None

    current_warnings = _current_warning_lookup(report_index_payload)
    rows = [
        _cleanup_row(warning, current_warnings)
        for warning in _records(warning_triage.get("warning_triage"))
    ]
    summary = _summary(warning_triage, rows, report_index_payload)
    status = _cleanup_status(summary)
    reader_brief = _reader_brief(status, summary, rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "cleanup_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "cleanup_only": True,
        "input_artifacts": {
            "report_index_warning_triage": _path_text(warning_triage_path),
            "report_index": _path_text(report_index_path),
        },
        "summary": summary,
        "warning_cleanup_rows": rows,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Cleanup report is read-only and does not apply report-index waivers.",
            "Stale or required daily artifacts must stay visible until regenerated.",
            "True blockers are not silently waived.",
        ],
        "next_action": reader_brief["next_action"],
    }


def validate_report_index_warning_cleanup_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    rows = _records(payload.get("warning_cleanup_rows"))
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_report_index_warning_cleanup",
    )
    _append_check(
        checks,
        blocking_issues,
        "status_enum",
        _text(payload.get("cleanup_status")) in REPORT_INDEX_WARNING_STATUSES,
        "cleanup_status must use the supported enum.",
        "restore_report_index_warning_cleanup_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "row_count_matches",
        _int(summary.get("triaged_warning_count")) == len(rows),
        "triaged_warning_count must match cleanup rows.",
        "regenerate_report_index_warning_cleanup",
    )
    _append_check(
        checks,
        blocking_issues,
        "active_count_matches",
        _int(summary.get("remaining_unwaived_count"))
        == len([row for row in rows if row.get("still_unwaived") is True]),
        "remaining_unwaived_count must match active unwaived cleanup rows.",
        "repair_report_index_warning_cleanup_counts",
    )
    _append_check(
        checks,
        blocking_issues,
        "no_silent_waivers",
        _int(summary.get("silent_waiver_count")) == 0
        and all(row.get("silent_waiver_detected") is False for row in rows),
        "Cleanup must not silently waive report-index warnings.",
        "remove_silent_waiver_or_restore_visible_warning",
    )
    _append_check(
        checks,
        blocking_issues,
        "row_required_fields",
        all(_cleanup_row_complete(row) for row in rows),
        "Every cleanup row must expose warning id, classification, visibility, and action.",
        "repair_report_index_warning_cleanup_rows",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_report_index_warning_cleanup_reader_brief",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Cleanup report must preserve read-only no-waiver safety boundary.",
        "restore_report_index_warning_cleanup_safety_boundary",
    )
    if _int(summary.get("remaining_unwaived_count")):
        warning_issues.append(
            {
                "issue_id": "report_index_warnings_remain_visible",
                "message": (
                    "Report-index warnings remain visible and require regeneration "
                    "or owner review."
                ),
                "count": _int(summary.get("remaining_unwaived_count")),
                "recommended_action": _text(payload.get("next_action")),
            }
        )
    if _int(summary.get("owner_review_count")):
        warning_issues.append(
            {
                "issue_id": "owner_review_required_for_visible_warnings",
                "message": (
                    "Some visible warnings require owner review before cleanup can be "
                    "closed."
                ),
                "count": _int(summary.get("owner_review_count")),
                "recommended_action": "review_report_index_warning_cleanup_rows",
            }
        )

    validation_status = FAIL_STATUS
    if not blocking_issues:
        validation_status = PASS_WITH_WARNINGS_STATUS if warning_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "warning_check_count": len(warning_issues),
        "triaged_warning_count": len(rows),
        "remaining_unwaived_count": _int(summary.get("remaining_unwaived_count")),
        "fixed_warning_count": _int(summary.get("fixed_warning_count")),
        "silent_waiver_count": _int(summary.get("silent_waiver_count")),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_cleanup_status": _text(payload.get("cleanup_status")),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": f"Report-index warning cleanup validation is {validation_status}.",
            "key_result": validation_status,
            "blocking_issues": _issue_list(blocking_issues, "issue_id", "message"),
            "warnings": _issue_list(warning_issues, "issue_id", "message"),
            "safety_boundary": "read-only validation; no waivers or state mutation.",
            "next_action": (
                "repair_report_index_warning_cleanup"
                if validation_status == FAIL_STATUS
                else "review_remaining_report_index_warnings"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _safety_boundary(),
    }


def write_report_index_warning_cleanup_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_report_index_warning_cleanup_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_report_index_warning_cleanup_markdown(payload), output_path)


def write_report_index_warning_cleanup_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_report_index_warning_cleanup_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(
        render_report_index_warning_cleanup_validation_markdown(payload),
        output_path,
    )


def render_report_index_warning_cleanup_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Report Index Warning Cleanup {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- cleanup_status: {payload.get('cleanup_status')}",
        f"- triaged_warning_count: {summary.get('triaged_warning_count')}",
        f"- remaining_unwaived_count: {summary.get('remaining_unwaived_count')}",
        f"- fixed_warning_count: {summary.get('fixed_warning_count')}",
        f"- owner_review_count: {summary.get('owner_review_count')}",
        f"- metadata_repair_count: {summary.get('metadata_repair_count')}",
        f"- stale_artifact_count: {summary.get('stale_artifact_count')}",
        f"- silent_waiver_count: {summary.get('silent_waiver_count')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Cleanup Rows",
        "",
        "|warning_id|report_id|classification|still_unwaived|cleanup_status|action|",
        "|---|---|---|---|---|---|",
    ]
    for row in _records(payload.get("warning_cleanup_rows")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    row.get("warning_id"),
                    row.get("report_id"),
                    row.get("warning_classification"),
                    row.get("still_unwaived"),
                    row.get("cleanup_status"),
                    row.get("recommended_action"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Safety Boundary", "", "- read-only cleanup; no silent waiver.", ""])
    return "\n".join(lines)


def render_report_index_warning_cleanup_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Report Index Warning Cleanup Validation {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_cleanup_status: {payload.get('source_cleanup_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- remaining_unwaived_count: {summary.get('remaining_unwaived_count')}",
        f"- silent_waiver_count: {summary.get('silent_waiver_count')}",
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


def _payload_or_latest(
    payload: Mapping[str, Any] | None,
    path: Path | None,
    latest_fn: Any,
    reports_dir: Path,
    label: str,
) -> tuple[Path | None, Mapping[str, Any]]:
    if payload is not None:
        return path, payload
    source_path = path or latest_fn(reports_dir)
    if source_path is None:
        raise FileNotFoundError(f"{label} JSON not found in {reports_dir}")
    return source_path, _read_json_mapping(source_path)


def _current_warning_lookup(report_index: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    lookup: dict[str, Mapping[str, Any]] = {}
    for report in _records(report_index.get("reports")):
        issue = _mapping(report.get("visibility_issue"))
        issue_id = _text(issue.get("issue_id"))
        if issue_id and _text(report.get("visibility_status")) == "WARNING":
            lookup[issue_id] = report
    return lookup


def _cleanup_row(
    warning: Mapping[str, Any],
    current_warnings: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    issue_id = _text(warning.get("issue_id"))
    current_entry = _mapping(current_warnings.get(issue_id))
    still_unwaived = bool(current_entry)
    classification = _text(warning.get("warning_classification"))
    cleanup_status = (
        "STILL_VISIBLE" if still_unwaived else "CLEARED_BY_REGENERATION_OR_INDEX_REFRESH"
    )
    if still_unwaived and classification == "true_blocker":
        cleanup_status = "STILL_VISIBLE_TRUE_BLOCKER"
    if still_unwaived and classification == "governance_warning":
        cleanup_status = "STILL_VISIBLE_OWNER_REVIEW"
    return {
        "warning_id": issue_id,
        "report_id": _text(warning.get("report_id")),
        "title": _text(warning.get("title")),
        "warning_classification": classification,
        "issue_status": _text(warning.get("issue_status")),
        "still_unwaived": still_unwaived,
        "cleanup_status": cleanup_status,
        "current_visibility_status": _text(current_entry.get("visibility_status")),
        "current_freshness_status": _text(
            current_entry.get("freshness_status"),
            _text(warning.get("freshness_status")),
        ),
        "latest_artifact_path": _text(
            current_entry.get("latest_artifact_path"),
            _text(warning.get("latest_artifact_path")),
        ),
        "metadata_repair_needed": classification in {
            "missing_metadata_warning",
            "documentation_inconsistency",
        },
        "owner_review_needed": classification in {
            "true_blocker",
            "governance_warning",
            "legacy_warning_candidate",
        },
        "stale_artifact_needs_regeneration": classification == "stale_artifact_warning"
        or _text(warning.get("issue_status")) == "STALE",
        "explicit_waiver_needed": _mapping(warning.get("waiver_requirements")).get("required")
        is True
        or _text(warning.get("proposed_action")) == "create_explicit_expiring_waiver",
        "silent_waiver_detected": False,
        "waiver_action": _text(warning.get("waiver_action"), "not_applied"),
        "recommended_action": _cleanup_action(warning, classification, still_unwaived),
        "production_effect": PRODUCTION_EFFECT,
    }


def _cleanup_action(
    warning: Mapping[str, Any],
    classification: str,
    still_unwaived: bool,
) -> str:
    if not still_unwaived:
        return "confirm_report_index_refresh_removed_warning_without_waiver"
    if classification == "true_blocker":
        return "regenerate_required_daily_artifact_or_keep_warning_visible"
    if classification == "governance_warning":
        return "regenerate_governance_artifact_or_record_owner_review"
    if classification == "stale_artifact_warning":
        return "regenerate_real_artifact_or_keep_warning_visible"
    if classification in {"missing_metadata_warning", "documentation_inconsistency"}:
        return "repair_report_metadata_then_rerun_report_index"
    return _text(warning.get("owner_action"), "review_report_index_warning")


def _summary(
    warning_triage: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    classification_counts = Counter(_text(row.get("warning_classification")) for row in rows)
    remaining = [row for row in rows if row.get("still_unwaived") is True]
    return {
        "triage_status": _text(warning_triage.get("triage_status")),
        "report_index_status": _text(report_index.get("status"), "MISSING"),
        "report_count": _int(_mapping(report_index.get("summary")).get("report_count")),
        "triaged_warning_count": len(rows),
        "remaining_unwaived_count": len(remaining),
        "fixed_warning_count": len(rows) - len(remaining),
        "explicit_waiver_count": _int(
            _mapping(report_index.get("summary")).get("explicit_waiver_count")
        ),
        "explicit_waiver_needed_count": len(
            [row for row in rows if row.get("explicit_waiver_needed") is True]
        ),
        "silent_waiver_count": len(
            [row for row in rows if row.get("silent_waiver_detected") is True]
        ),
        "metadata_repair_count": len(
            [row for row in remaining if row.get("metadata_repair_needed") is True]
        ),
        "owner_review_count": len(
            [row for row in remaining if row.get("owner_review_needed") is True]
        ),
        "stale_artifact_count": len(
            [row for row in remaining if row.get("stale_artifact_needs_regeneration") is True]
        ),
        "classification_counts": dict(classification_counts),
        "next_action": (
            "rerun_real_artifact_generation_or_keep_report_index_warnings_visible"
            if remaining
            else "rerun_report_index_and_validate_no_unwaived_warnings"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _cleanup_status(summary: Mapping[str, Any]) -> str:
    if _int(summary.get("remaining_unwaived_count")) == 0:
        return REPORT_INDEX_WARNINGS_CLEARED
    if _int(summary.get("owner_review_count")):
        return REPORT_INDEX_WARNINGS_MANUAL_REVIEW_REQUIRED
    return REPORT_INDEX_WARNINGS_REMAIN


def _reader_brief(
    status: str,
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    remaining_rows = [row for row in rows if row.get("still_unwaived") is True]
    return {
        "summary": (
            f"Report-index warning cleanup is {status}: "
            f"{_int(summary.get('remaining_unwaived_count'))} warnings remain visible."
        ),
        "key_result": status,
        "blocking_issues": _issue_list(remaining_rows, "warning_id", "cleanup_status"),
        "warnings": (
            "none"
            if not remaining_rows
            else "unwaived report-index warnings remain visible"
        ),
        "safety_boundary": (
            "Cleanup is read-only and does not create silent waivers, mutate report index, "
            "or hide stale required artifacts."
        ),
        "next_action": _text(summary.get("next_action")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _cleanup_row_complete(row: Mapping[str, Any]) -> bool:
    return all(
        _text(row.get(field))
        for field in (
            "warning_id",
            "report_id",
            "warning_classification",
            "cleanup_status",
            "recommended_action",
        )
    ) and row.get("silent_waiver_detected") is False


def _reader_brief_complete(value: Any) -> bool:
    reader_brief = _mapping(value)
    return all(bool(_text(reader_brief.get(field))) for field in CORE_READER_BRIEF_FIELDS)


def _safety_boundary_valid(value: Any) -> bool:
    safety = _mapping(value)
    return (
        _text(safety.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("does_not_create_waivers") is True
        and safety.get("does_not_modify_report_index") is True
        and safety.get("does_not_hide_warnings") is True
        and safety.get("production_state_mutated") is False
        and safety.get("live_trading_allowed") is False
    )


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_warning_triage_and_report_index_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "does_not_create_waivers": True,
        "does_not_modify_report_index": True,
        "does_not_hide_warnings": True,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "live_trading_allowed": False,
    }


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
) -> None:
    check = {
        "check_id": check_id,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "message": message,
        "recommended_action": recommended_action,
    }
    checks.append(check)
    if not passed:
        blocking_issues.append(
            {
                "issue_id": check_id,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _issue_list(
    records: Sequence[Mapping[str, Any]],
    key_field: str,
    value_field: str,
) -> str:
    if not records:
        return "none"
    return "; ".join(
        f"{_text(record.get(key_field))}:{_text(record.get(value_field))}"
        for record in records[:5]
    )


def _path_text(path: Path | None) -> str:
    return "" if path is None else str(path)


def _write_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def _write_text(text: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path
