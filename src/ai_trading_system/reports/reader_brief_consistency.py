from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
REPORT_TYPE = "reader_brief_consistency_pack"
VALIDATION_REPORT_TYPE = "reader_brief_consistency_validation"
PRODUCTION_EFFECT = "none"

PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

STANDARD_READER_BRIEF_SECTIONS: tuple[str, ...] = (
    "summary",
    "key_result",
    "blocking_issues",
    "warnings",
    "safety_boundary",
    "next_action",
)
CORE_DAILY_READER_BRIEF_SECTIONS = frozenset(
    {"summary", "key_result", "safety_boundary", "next_action"}
)
SECTION_LABELS: dict[str, str] = {
    "summary": "Summary",
    "key_result": "Key Result",
    "blocking_issues": "Blocking Issues",
    "warnings": "Warnings",
    "safety_boundary": "Safety Boundary",
    "next_action": "Next Action",
}
SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "summary": (
        "summary",
        "human_readable_summary",
        "narrative_summary",
        "summary_sentence",
        "reader_summary",
    ),
    "key_result": (
        "key_result",
        "result",
        "output_decision",
        "decision",
        "decision_state",
        "status",
        "report_status",
        "validation_status",
        "inventory_status",
        "quality_status",
        "consistency_status",
    ),
    "blocking_issues": (
        "blocking_issues",
        "blocking_issue_count",
        "blocking_quality_issues",
        "blocking_reasons",
        "blocking_failures",
        "failed_checks",
    ),
    "warnings": (
        "warnings",
        "warning_issues",
        "warning_issue_count",
        "warning_quality_issues",
        "warning_check_count",
    ),
    "safety_boundary": (
        "safety_boundary",
        "production_effect",
        "broker_effect",
        "broker_action_allowed",
        "broker_action_taken",
        "order_effect",
        "order_ticket_generated",
        "manual_review_only",
        "not_official_target_weights",
    ),
    "next_action": (
        "next_action",
        "next_required_action",
        "recommended_next_step",
        "recommended_action",
        "recommended_owner_action",
        "owner_action",
        "action_checklist",
    ),
}
DECISION_KEYS = SECTION_ALIASES["key_result"] + (
    "final_action",
    "final_owner_action",
    "owner_decision",
)
UNCLEAR_DECISION_VALUES = {"", "UNKNOWN", "UNCLEAR", "N/A", "NONE", "TBD"}


def default_reader_brief_consistency_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"reader_brief_consistency_pack_{as_of.isoformat()}.json"


def default_reader_brief_consistency_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"reader_brief_consistency_pack_{as_of.isoformat()}.md"


def default_reader_brief_consistency_validation_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"reader_brief_consistency_validation_{as_of.isoformat()}.json"


def default_reader_brief_consistency_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"reader_brief_consistency_validation_{as_of.isoformat()}.md"


def latest_reader_brief_consistency_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "reader_brief_consistency_pack_", ".json")


def build_reader_brief_consistency_payload(
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

    report_checks = [
        _check_report_reader_brief_sections(
            report=report,
            project_root=project_root,
        )
        for report in _records(report_index_payload.get("reports"))
        if report.get("include_in_reader_brief") is True
    ]
    missing_sections = _dedupe_issues(
        issue
        for check in report_checks
        for issue in _records(check.get("missing_sections"))
    )
    unclear_decisions = _dedupe_issues(
        issue
        for check in report_checks
        for issue in _records(check.get("unclear_decision_issues"))
    )
    blocking_issues = _dedupe_issues(
        issue
        for check in report_checks
        for issue in _records(check.get("blocking_issues"))
    )
    warning_issues = _dedupe_issues(
        issue
        for check in report_checks
        for issue in _records(check.get("warning_issues"))
    )
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    covered_reports = [
        check for check in report_checks if check.get("section_source") != "missing_artifact"
    ]
    full_coverage = [
        check
        for check in covered_reports
        if not _records(check.get("missing_sections"))
        and not _records(check.get("unclear_decision_issues"))
    ]
    summary = {
        "checked_report_count": len(report_checks),
        "available_report_count": len(covered_reports),
        "full_coverage_report_count": len(full_coverage),
        "missing_section_count": len(missing_sections),
        "unclear_decision_count": len(unclear_decisions),
        "blocking_issue_count": len(blocking_issues),
        "warning_issue_count": len(warning_issues),
        "standard_section_count": len(STANDARD_READER_BRIEF_SECTIONS),
        "daily_reader_brief_core_missing_count": len(
            [
                issue
                for issue in missing_sections
                if issue.get("report_id") == "reader_brief"
                and issue.get("section") in CORE_DAILY_READER_BRIEF_SECTIONS
            ]
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "consistency_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": (
            "Validate that Reader Brief-facing reports expose consistent summary, result, "
            "issue, safety, and next-action language."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
        },
        "output_decision": status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "The consistency pack reads existing artifacts only.",
            "Legacy artifacts with missing standard sections are reported as warnings.",
            "The pack does not rewrite historical reports or rerun upstream commands.",
        ],
        "next_action": _next_action(status),
        "standard_sections": [
            {"section": section, "label": SECTION_LABELS[section]}
            for section in STANDARD_READER_BRIEF_SECTIONS
        ],
        "summary": summary,
        "report_checks": report_checks,
        "missing_sections": missing_sections,
        "unclear_decision_issues": unclear_decisions,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(status, summary, blocking_issues, warning_issues),
        "methodology": {
            "mode": "read_existing_report_index_and_artifacts_only",
            "standard_sections": list(STANDARD_READER_BRIEF_SECTIONS),
            "daily_reader_brief_core_sections": sorted(CORE_DAILY_READER_BRIEF_SECTIONS),
            "legacy_artifact_gaps_are_warnings": True,
            "does_not_rewrite_historical_artifacts": True,
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_reader_brief_consistency_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    source_status = _text(payload.get("consistency_status"), _text(payload.get("status")))
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        "BLOCKING",
        f"report_type must be {REPORT_TYPE}.",
        "rerun_reader_brief_consistency_pack",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "BLOCKING",
        "Reader Brief consistency pack must be production_effect=none.",
        "regenerate_consistency_pack_without_production_mutation",
    )
    _append_check(
        checks,
        blocking_issues,
        "standard_sections_declared",
        set(_section_ids(payload.get("standard_sections"))) == set(STANDARD_READER_BRIEF_SECTIONS),
        "BLOCKING",
        "Consistency pack must declare the six standard Reader Brief sections.",
        "restore_standard_reader_brief_section_contract",
    )
    _append_check(
        checks,
        blocking_issues,
        "daily_reader_brief_core_sections",
        _int(summary.get("daily_reader_brief_core_missing_count")) == 0,
        "BLOCKING",
        "Daily Reader Brief must expose summary, key result, safety boundary, and next action.",
        "regenerate_reader_brief_with_standard_core_sections",
    )
    _append_check(
        checks,
        blocking_issues,
        "source_not_fail",
        source_status != FAIL_STATUS,
        "BLOCKING",
        f"source consistency_status is {source_status}.",
        "fix_reader_brief_consistency_blockers",
    )
    _append_check(
        checks,
        warning_issues,
        "legacy_section_gaps_visible",
        _int(summary.get("missing_section_count")) == 0,
        "WARNING",
        "Some Reader Brief-facing artifacts are missing standard sections.",
        "update_new_report_templates_to_emit_standard_reader_brief_sections",
    )
    _append_check(
        checks,
        warning_issues,
        "decision_state_clear",
        _int(summary.get("unclear_decision_count")) == 0,
        "WARNING",
        "Some Reader Brief-facing artifacts have unclear decision language.",
        "add_clear_status_or_output_decision_to_reader_brief_sections",
    )
    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == "FAIL"]),
        "warning_check_count": len(
            [
                check
                for check in checks
                if check["status"] == "FAIL" and check["severity"] == "WARNING"
            ]
        ),
        "source_checked_report_count": _int(summary.get("checked_report_count")),
        "source_available_report_count": _int(summary.get("available_report_count")),
        "source_full_coverage_report_count": _int(summary.get("full_coverage_report_count")),
        "source_missing_section_count": _int(summary.get("missing_section_count")),
        "source_unclear_decision_count": _int(summary.get("unclear_decision_count")),
        "checked_report_count": _int(summary.get("checked_report_count")),
        "available_report_count": _int(summary.get("available_report_count")),
        "full_coverage_report_count": _int(summary.get("full_coverage_report_count")),
        "missing_section_count": _int(summary.get("missing_section_count")),
        "unclear_decision_count": _int(summary.get("unclear_decision_count")),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "source_consistency_status": source_status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": "Validate the Reader Brief consistency pack fail-closed contract.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation reads an existing consistency pack only.",
            (
                "Legacy section gaps remain warnings unless daily Reader Brief core "
                "sections are missing."
            ),
        ],
        "next_action": _validation_next_action(status),
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(
            status,
            validation_summary,
            blocking_issues,
            warning_issues,
        ),
        "methodology": {
            "mode": "read_existing_reader_brief_consistency_pack_only",
            "production_effect": PRODUCTION_EFFECT,
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
        },
    }


def write_reader_brief_consistency_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_reader_brief_consistency_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_reader_brief_consistency_markdown(payload), encoding="utf-8")
    return output_path


def write_reader_brief_consistency_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_reader_brief_consistency_json(payload, output_path)


def write_reader_brief_consistency_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_reader_brief_consistency_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_reader_brief_consistency_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    reader = _mapping(payload.get("reader_brief"))
    lines = [
        f"# Reader Brief Consistency Pack {payload.get('as_of')}",
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
        f"- checked reports：{summary.get('checked_report_count')}",
        f"- full coverage：{summary.get('full_coverage_report_count')}",
        f"- missing sections：{summary.get('missing_section_count')}",
        f"- unclear decisions：{summary.get('unclear_decision_count')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- next_action：{_text(payload.get('next_action'))}",
        "",
        "## Standard Sections",
        "",
        "|section|label|",
        "|---|---|",
    ]
    for section in _records(payload.get("standard_sections")):
        lines.append(f"|{_markdown_cell(section.get('section'))}|{_markdown_cell(section.get('label'))}|")
    lines.extend(
        [
            "",
            "## Missing Sections",
            "",
            "|report_id|section|severity|artifact_path|",
            "|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("missing_sections"))[:120]:
        lines.append(
            f"|{_markdown_cell(issue.get('report_id'))}|"
            f"{_markdown_cell(issue.get('section'))}|"
            f"{_markdown_cell(issue.get('severity'))}|"
            f"{_markdown_cell(issue.get('artifact_path'))}|"
        )
    if not _records(payload.get("missing_sections")):
        lines.append("|NONE||||")
    lines.extend(
        [
            "",
            "## Unclear Decision Issues",
            "",
            "|issue_id|report_id|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("unclear_decision_issues"))[:80]:
        lines.append(_issue_row(issue))
    if not _records(payload.get("unclear_decision_issues")):
        lines.append("|NONE|reader_brief_consistency|无 unclear decision。||")
    lines.extend(
        [
            "",
            "## Methodology",
            "",
            "本 pack 只读取 report index 和既有 report artifacts；不重写历史 artifact、"
            "不运行上游、不刷新数据、不修改 production state。",
            "",
        ]
    )
    return "\n".join(lines)


def render_reader_brief_consistency_validation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Reader Brief Consistency Validation {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('validation_status'), 'UNKNOWN')}",
        f"- source_consistency_status：{_text(payload.get('source_consistency_status'))}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- checks：{summary.get('check_count')}",
        f"- failed：{summary.get('failed_check_count')}",
        f"- warnings：{summary.get('warning_check_count')}",
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
            "|issue_id|report_id|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("blocking_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("blocking_issues")):
        lines.append("|NONE|reader_brief_consistency_validation|无阻断项。||")
    lines.extend(
        [
            "",
            "## Warning Issues",
            "",
            "|issue_id|report_id|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("warning_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("warning_issues")):
        lines.append("|NONE|reader_brief_consistency_validation|无 warning。||")
    lines.append("")
    return "\n".join(lines)


def _check_report_reader_brief_sections(
    *,
    report: Mapping[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    report_id = _text(report.get("report_id"), "UNKNOWN_REPORT")
    artifact_path = _resolve_path(_text(report.get("latest_artifact_path")), project_root)
    if artifact_path is None or not artifact_path.exists():
        return {
            "report_id": report_id,
            "artifact_path": "" if artifact_path is None else str(artifact_path),
            "section_source": "missing_artifact",
            "section_checks": {
                section: "NOT_CHECKED" for section in STANDARD_READER_BRIEF_SECTIONS
            },
            "decision_state": "NOT_CHECKED",
            "missing_sections": [],
            "unclear_decision_issues": [],
            "blocking_issues": [],
            "warning_issues": [],
        }

    payload: Mapping[str, Any] | None = None
    text_content = ""
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    try:
        if artifact_path.suffix.lower() == ".json":
            raw = json.loads(artifact_path.read_text(encoding="utf-8"))
            if not isinstance(raw, Mapping):
                raise ValueError("JSON artifact must be an object")
            payload = raw
        elif artifact_path.suffix.lower() in {".md", ".html", ".htm"}:
            text_content = artifact_path.read_text(encoding="utf-8")
        else:
            warning_issues.append(
                _issue(
                    "unsupported_reader_brief_consistency_artifact_type",
                    report_id=report_id,
                    artifact_path=artifact_path,
                    severity="WARNING",
                    message="Artifact type is not JSON/Markdown/HTML for section scan.",
                    recommended_action="point_report_registry_to_reader_visible_json_or_markdown",
                )
            )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        blocking_issues.append(
            _issue(
                "unreadable_reader_brief_consistency_artifact",
                report_id=report_id,
                artifact_path=artifact_path,
                severity="BLOCKING",
                message=f"Artifact cannot be read for Reader Brief consistency scan: {exc}",
                recommended_action="repair_or_regenerate_report_artifact",
            )
        )

    section_source, section_values = _extract_sections(
        report_id=report_id,
        payload=payload,
        text_content=text_content,
    )
    section_checks = {
        section: "PASS" if _section_present(section_values, section) else "MISSING"
        for section in STANDARD_READER_BRIEF_SECTIONS
    }
    missing_sections = [
        _missing_section_issue(
            report_id=report_id,
            artifact_path=artifact_path,
            section=section,
            severity=_missing_section_severity(report_id, section),
        )
        for section, status in section_checks.items()
        if status == "MISSING"
    ]
    decision_state = _decision_state(payload, section_values)
    unclear_decision_issues = []
    if _unclear_decision(decision_state):
        unclear_decision_issues.append(
            _issue(
                "unclear_reader_brief_decision_state",
                report_id=report_id,
                artifact_path=artifact_path,
                severity="BLOCKING" if report_id == "reader_brief" else "WARNING",
                message="Reader Brief-facing artifact lacks clear status or decision language.",
                recommended_action="add_output_decision_status_or_key_result",
            )
        )
    production_effect = _text(report.get("artifact_production_effect"))
    if payload is not None:
        production_effect = _text(payload.get("production_effect"), production_effect)
    if production_effect and production_effect not in {PRODUCTION_EFFECT, "read_only", "advisory"}:
        blocking_issues.append(
            _issue(
                "reader_brief_consistency_production_effect_risk",
                report_id=report_id,
                artifact_path=artifact_path,
                severity="BLOCKING",
                message=f"Reader Brief-facing artifact production_effect is {production_effect}.",
                recommended_action="remove_production_effect_risk_before_reader_use",
            )
        )
    blocking_issues.extend(
        issue for issue in missing_sections if issue.get("severity") == "BLOCKING"
    )
    blocking_issues.extend(
        issue for issue in unclear_decision_issues if issue.get("severity") == "BLOCKING"
    )
    warning_issues.extend(
        issue for issue in missing_sections if issue.get("severity") != "BLOCKING"
    )
    warning_issues.extend(
        issue for issue in unclear_decision_issues if issue.get("severity") != "BLOCKING"
    )
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    return {
        "report_id": report_id,
        "title": _text(report.get("title")),
        "artifact_path": str(artifact_path),
        "section_source": section_source,
        "reader_brief_consistency_status": status,
        "section_checks": section_checks,
        "decision_state": decision_state,
        "missing_sections": missing_sections,
        "unclear_decision_issues": unclear_decision_issues,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
    }


def _extract_sections(
    *,
    report_id: str,
    payload: Mapping[str, Any] | None,
    text_content: str,
) -> tuple[str, dict[str, Any]]:
    if payload is not None and report_id == "reader_brief":
        return "daily_reader_brief_payload", _daily_reader_brief_sections(payload)
    if payload is not None:
        reader = payload.get("reader_brief")
        if isinstance(reader, Mapping):
            return "reader_brief_mapping", _section_values_from_mapping(reader)
        if isinstance(reader, str) and reader.strip():
            return "reader_brief_text", _section_values_from_text(reader)
        section_text = payload.get("reader_brief_section")
        if isinstance(section_text, str) and section_text.strip():
            return "reader_brief_section_text", _section_values_from_text(section_text)
        return "derived_top_level_payload", _section_values_from_mapping(payload)
    if text_content:
        return "artifact_text", _section_values_from_text(text_content)
    return "no_reader_brief_surface", {}


def _daily_reader_brief_sections(payload: Mapping[str, Any]) -> dict[str, Any]:
    narrative = _mapping(payload.get("narrative_executive_summary"))
    status_panel = _mapping(payload.get("status_panel"))
    executive = _mapping(payload.get("executive_decision"))
    missing = _mapping(payload.get("missing_limited_artifact_impact"))
    queue = _mapping(payload.get("manual_review_queue"))
    warnings = _records(missing.get("items")) + _records(queue.get("items"))
    return {
        "summary": _first_text(
            narrative.get("today_conclusion"),
            narrative.get("why"),
            payload.get("summary"),
        ),
        "key_result": _first_text(
            status_panel.get("decision_usability"),
            executive.get("action"),
            payload.get("status"),
        ),
        "blocking_issues": (
            f"blocking={_int(missing.get('blocking_count'))}; "
            f"manual_review={len(_records(queue.get('items')))}"
        ),
        "warnings": warnings if warnings else "none",
        "safety_boundary": (
            f"production_effect={_text(payload.get('production_effect'), PRODUCTION_EFFECT)}; "
            f"not_trade_instruction={_mapping(payload.get('executive_decision')).get('not_trade_instruction')}"
        ),
        "next_action": _first_text(
            executive.get("recommended_action"),
            _first_action(payload.get("action_checklist")),
            payload.get("next_action"),
        ),
    }


def _section_values_from_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    return {
        section: _first_matching_value(
            mapping,
            aliases,
            allow_empty=section in {"blocking_issues", "warnings"},
        )
        for section, aliases in SECTION_ALIASES.items()
    }


def _section_values_from_text(text: str) -> dict[str, Any]:
    lowered = text.lower()
    return {
        section: (
            text
            if label.lower() in lowered or _text_patterns_present(section, lowered)
            else _MISSING
        )
        for section, label in SECTION_LABELS.items()
    }


def _text_patterns_present(section: str, lowered: str) -> bool:
    patterns = {
        "summary": ("summary", "摘要", "概览"),
        "key_result": ("key result", "result", "decision", "结论", "状态"),
        "blocking_issues": ("blocking issues", "blocker", "阻断", "阻塞"),
        "warnings": ("warnings", "warning", "警告", "风险"),
        "safety_boundary": ("safety boundary", "production_effect", "broker", "安全边界"),
        "next_action": ("next action", "next step", "owner action", "下一步", "建议"),
    }
    return any(pattern in lowered for pattern in patterns[section])


def _section_present(section_values: Mapping[str, Any], section: str) -> bool:
    if section in {"blocking_issues", "warnings"}:
        return section in section_values and section_values.get(section) is not _MISSING
    return _non_empty(section_values.get(section))


def _decision_state(payload: Mapping[str, Any] | None, section_values: Mapping[str, Any]) -> str:
    if payload is not None:
        value = _first_matching_value(payload, DECISION_KEYS, allow_empty=False)
        if _text(value):
            return _text(value)
    return _text(section_values.get("key_result"))


def _unclear_decision(value: str) -> bool:
    return _text(value).upper() in UNCLEAR_DECISION_VALUES


def _missing_section_severity(report_id: str, section: str) -> str:
    if report_id == "reader_brief" and section in CORE_DAILY_READER_BRIEF_SECTIONS:
        return "BLOCKING"
    return "WARNING"


def _missing_section_issue(
    *,
    report_id: str,
    artifact_path: Path,
    section: str,
    severity: str,
) -> dict[str, Any]:
    return _issue(
        f"missing_reader_brief_section_{section}",
        report_id=report_id,
        artifact_path=artifact_path,
        severity=severity,
        message=f"Reader Brief-facing artifact is missing {SECTION_LABELS[section]}.",
        recommended_action="update_report_template_to_emit_standard_reader_brief_section",
        section=section,
    )


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
) -> None:
    status = "PASS" if passed else "FAIL"
    check = {
        "check_id": check_id,
        "status": status,
        "severity": severity,
        "message": message,
        "recommended_action": "" if passed else recommended_action,
    }
    checks.append(check)
    if not passed:
        issues.append(
            {
                "issue_id": check_id,
                "report_id": "reader_brief_consistency",
                "severity": severity,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _issue(
    issue_id: str,
    *,
    report_id: str,
    severity: str,
    message: str,
    recommended_action: str,
    artifact_path: Path | None = None,
    section: str = "",
) -> dict[str, Any]:
    return {
        "issue_id": issue_id,
        "report_id": report_id,
        "severity": severity,
        "section": section,
        "message": message,
        "recommended_action": recommended_action,
        "artifact_path": "" if artifact_path is None else str(artifact_path),
    }


def _reader_brief(
    status: str,
    summary: Mapping[str, Any],
    blocking_issues: Sequence[Mapping[str, Any]],
    warning_issues: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            "Reader Brief consistency pack checked "
            f"{_int(summary.get('checked_report_count'))} Reader Brief-facing reports."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if not blocking_issues
            else f"{len(blocking_issues)} blocking Reader Brief consistency issue(s)."
        ),
        "warnings": (
            "none"
            if not warning_issues
            else f"{len(warning_issues)} warning Reader Brief consistency issue(s)."
        ),
        "safety_boundary": (
            "Read-only consistency audit; no historical artifact rewrite, no upstream rerun, "
            "no data refresh, no official target weights, no broker/order, production_effect=none."
        ),
        "next_action": _next_action(status),
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_report_index_and_artifacts_only",
        "does_not_rewrite_historical_artifacts": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_modify_scores_weights_or_gates": True,
        "official_target_weights": False,
        "broker_action_allowed": False,
        "order_ticket_generated": False,
        "production_state_mutated": False,
        "production_effect": PRODUCTION_EFFECT,
    }


def _next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "fix_reader_brief_consistency_blockers_before_governance_pack"
    if status == WARN_STATUS:
        return "update_new_report_templates_to_emit_standard_reader_brief_sections"
    return "continue_reader_brief_consistency_monitoring"


def _validation_next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "fix_reader_brief_consistency_validation_blockers"
    if status == WARN_STATUS:
        return "review_reader_brief_section_gap_warnings"
    return "reader_brief_consistency_validation_passed"


def _read_json_mapping(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"JSON artifact not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON artifact cannot be parsed: {path}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return raw


def _resolve_path(value: str, project_root: Path) -> Path | None:
    if not value:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    return path


def _first_matching_value(value: Any, aliases: Sequence[str], *, allow_empty: bool) -> Any:
    normalized_aliases = {_normalize_key(alias) for alias in aliases}
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if _normalize_key(_text(key)) in normalized_aliases:
                if allow_empty or _non_empty(nested):
                    return nested
            found = _first_matching_value(nested, aliases, allow_empty=allow_empty)
            if allow_empty and found is not _MISSING:
                return found
            if not allow_empty and _non_empty(found):
                return found
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            found = _first_matching_value(item, aliases, allow_empty=allow_empty)
            if allow_empty and found is not _MISSING:
                return found
            if not allow_empty and _non_empty(found):
                return found
    return _MISSING


class _Missing:
    pass


_MISSING = _Missing()


def _first_text(*values: Any) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _first_action(actions: Any) -> str:
    for item in _records(actions):
        text = _first_text(item.get("action"), item.get("label"), item.get("recommended_action"))
        if text:
            return text
    return ""


def _non_empty(value: Any) -> bool:
    if value is _MISSING:
        return False
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        return any(_non_empty(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return True
    return True


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _section_ids(value: Any) -> list[str]:
    return [_text(item.get("section")) for item in _records(value) if _text(item.get("section"))]


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _text(value: Any, default: str = "") -> str:
    if value is _MISSING:
        return default
    if value is None:
        return default
    return str(value).strip()


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _issue_row(issue: Mapping[str, Any]) -> str:
    return (
        f"|{_markdown_cell(issue.get('issue_id'))}|"
        f"{_markdown_cell(issue.get('report_id'))}|"
        f"{_markdown_cell(issue.get('message'))}|"
        f"{_markdown_cell(issue.get('recommended_action'))}|"
    )


def _dedupe_issues(values: Sequence[Mapping[str, Any]] | Any) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    output: list[dict[str, Any]] = []
    for item in values:
        if not isinstance(item, Mapping):
            continue
        key = (
            _text(item.get("issue_id")),
            _text(item.get("report_id")),
            _text(item.get("section")),
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(dict(item))
    return output


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    if not output_dir.exists():
        return None
    candidates: list[tuple[str, Path]] = []
    for path in output_dir.glob(f"{prefix}*{suffix}"):
        middle = path.name.removeprefix(prefix).removesuffix(suffix)
        try:
            date.fromisoformat(middle)
        except ValueError:
            continue
        candidates.append((middle, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]
