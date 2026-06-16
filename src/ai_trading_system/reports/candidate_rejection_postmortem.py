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
    _list_values,
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
REPORT_TYPE = "candidate_rejection_postmortem_template"
VALIDATION_REPORT_TYPE = "candidate_rejection_postmortem_template_validation"

TEMPLATE_READY = "TEMPLATE_READY"
TEMPLATE_BLOCKED = "TEMPLATE_BLOCKED"
POSTMORTEM_VALID = "POSTMORTEM_VALID"
POSTMORTEM_INVALID = "POSTMORTEM_INVALID"
NO_POSTMORTEM_RECORD_PROVIDED = "NO_POSTMORTEM_RECORD_PROVIDED"

PASS_STATUS = "PASS"
FAIL_STATUS = "FAIL"

REQUIRED_SECTIONS: tuple[dict[str, Any], ...] = (
    {
        "section_id": "candidate_summary",
        "title": "Candidate Summary",
        "required_fields": ("candidate_id", "candidate_name", "rejection_decision_source"),
        "field_type": "mapping",
    },
    {
        "section_id": "reason_for_rejection",
        "title": "Reason For Rejection",
        "required_fields": ("primary_reason", "rejection_category", "decision_reference"),
        "field_type": "mapping",
    },
    {
        "section_id": "failed_evidence_gates",
        "title": "Failed Evidence Gates",
        "required_fields": ("gate_id", "status", "reason", "source_artifact"),
        "field_type": "list",
    },
    {
        "section_id": "failed_stress_scenarios",
        "title": "Failed Stress Scenarios",
        "required_fields": ("scenario_id", "failure_mode", "severity", "source_artifact"),
        "field_type": "list",
    },
    {
        "section_id": "data_quality_issues",
        "title": "Data Quality Issues",
        "required_fields": ("issue_id", "status", "impact", "source_artifact"),
        "field_type": "list",
    },
    {
        "section_id": "safety_boundary_issues",
        "title": "Safety Boundary Issues",
        "required_fields": ("issue_id", "status", "impact", "source_artifact"),
        "field_type": "list",
    },
    {
        "section_id": "revisit_assessment",
        "title": "Whether Idea Can Be Revisited",
        "required_fields": ("can_revisit", "revisit_condition", "owner_review_required"),
        "field_type": "mapping",
    },
    {
        "section_id": "lessons_learned",
        "title": "Lessons Learned",
        "required_fields": ("lesson", "follow_up_task"),
        "field_type": "list",
    },
)
REQUIRED_SECTION_IDS = tuple(_text(section["section_id"]) for section in REQUIRED_SECTIONS)
LIST_SECTION_IDS = tuple(
    _text(section["section_id"])
    for section in REQUIRED_SECTIONS
    if section.get("field_type") == "list"
)
CONTEXT_REPORT_IDS = (
    "paper_shadow_promotion_board",
    "owner_decision_audit_log",
    "research_monthly_review_pack",
    "research_safety_boundary_audit",
)


def default_candidate_rejection_postmortem_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"candidate_rejection_postmortem_template_{as_of.isoformat()}.json"


def default_candidate_rejection_postmortem_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"candidate_rejection_postmortem_template_{as_of.isoformat()}.md"


def default_candidate_rejection_postmortem_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return (
        output_dir
        / f"candidate_rejection_postmortem_template_validation_{as_of.isoformat()}.json"
    )


def default_candidate_rejection_postmortem_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return (
        output_dir
        / f"candidate_rejection_postmortem_template_validation_{as_of.isoformat()}.md"
    )


def latest_candidate_rejection_postmortem_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "candidate_rejection_postmortem_template_", ".json")


def build_candidate_rejection_postmortem_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    postmortem_json_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    if report_index_payload is None:
        source_path = report_index_path or (
            project_root / "outputs" / "reports" / f"report_index_{as_of.isoformat()}.json"
        )
        report_index_payload = _read_json_mapping(source_path)
        report_index_path = source_path

    context = _source_context(report_index_payload, project_root=project_root)
    postmortem_record = (
        _read_optional_json_mapping(postmortem_json_path) if postmortem_json_path else {}
    )
    filled_validation = _validate_filled_postmortem(postmortem_record)
    summary = _summary(context, postmortem_record, filled_validation)
    reader_brief = _reader_brief(summary)
    template_status = (
        TEMPLATE_BLOCKED if filled_validation["status"] == POSTMORTEM_INVALID else TEMPLATE_READY
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": template_status,
        "template_status": template_status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "purpose": (
            "Provide a candidate rejection postmortem template and validate an optional "
            "filled postmortem record without rejecting candidates or changing state."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            "filled_postmortem_json": (
                "" if postmortem_json_path is None else str(postmortem_json_path)
            ),
            **{key: value.get("artifact_path", "") for key, value in context.items()},
        },
        "output_decision": template_status,
        "summary": summary,
        "required_sections": _required_section_contract(),
        "blank_postmortem_template": _blank_postmortem_template(
            as_of=as_of,
            context=context,
        ),
        "filled_postmortem_record": dict(postmortem_record),
        "filled_postmortem_validation": filled_validation,
        "source_context": context,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "The template does not reject candidates and does not mutate candidate state.",
            "No filled postmortem record is fabricated when a real rejection is absent.",
            "Filled postmortems remain manual governance records only.",
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_existing_report_index_and_optional_postmortem_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_candidate_rejection_postmortem_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    filled_validation = _mapping(payload.get("filled_postmortem_validation"))
    section_ids = {
        _text(section.get("section_id"))
        for section in _records(payload.get("required_sections"))
    }

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_candidate_rejection_postmortem_template",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "Candidate rejection postmortem template must be production_effect=none.",
        "restore_research_only_postmortem_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_sections_present",
        set(REQUIRED_SECTION_IDS).issubset(section_ids),
        "All required postmortem sections must be represented.",
        "restore_required_postmortem_section_contract",
    )
    template = _mapping(payload.get("blank_postmortem_template"))
    _append_check(
        checks,
        blocking_issues,
        "blank_template_contains_required_keys",
        set(REQUIRED_SECTION_IDS).issubset(set(template)),
        "Blank postmortem template must contain every required section key.",
        "regenerate_blank_postmortem_template",
    )
    _append_check(
        checks,
        blocking_issues,
        "filled_postmortem_valid_if_provided",
        _text(filled_validation.get("status")) != POSTMORTEM_INVALID,
        "Filled postmortem record must pass required section and safety checks.",
        "repair_filled_postmortem_before_owner_archive",
        details={"filled_status": _text(filled_validation.get("status"))},
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_state_mutation",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("production_state_mutated") is False
            and safety.get("official_target_weights_generated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
        ),
        (
            "Postmortem template must not mutate candidate, shadow, production, "
            "broker, or order state."
        ),
        "restore_postmortem_safety_boundary",
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
        "restore_postmortem_reader_brief_fields",
    )

    validation_status = FAIL_STATUS if blocking_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "blocking_issue_count": len(blocking_issues),
        "postmortem_record_provided": summary.get("postmortem_record_provided") is True,
        "filled_postmortem_status": _text(
            filled_validation.get("status"),
            NO_POSTMORTEM_RECORD_PROVIDED,
        ),
        "required_section_count": len(REQUIRED_SECTION_IDS),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "purpose": "Validate candidate rejection postmortem template and optional filled record.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": validation_status,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "source_summary": dict(summary),
        "source_filled_postmortem_validation": dict(filled_validation),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation does not reject candidates or write archive state.",
            "A valid postmortem record is manual governance evidence only.",
        ],
        "next_action": (
            "use_template_when_owner_records_rejection_postmortem"
            if validation_status == PASS_STATUS
            else "repair_postmortem_template_or_filled_record"
        ),
        "reader_brief": _reader_brief(summary),
        "methodology": {
            "collector_mode": "validate_existing_candidate_rejection_postmortem_template_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_candidate_rejection_postmortem_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_candidate_rejection_postmortem_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_candidate_rejection_postmortem_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def write_candidate_rejection_postmortem_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_candidate_rejection_postmortem_json(payload, output_path)


def write_candidate_rejection_postmortem_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_candidate_rejection_postmortem_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_candidate_rejection_postmortem_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Candidate Rejection Postmortem Template {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- template_status: {payload.get('template_status')}",
        f"- postmortem_record_provided: {summary.get('postmortem_record_provided')}",
        f"- filled_postmortem_status: {summary.get('filled_postmortem_status')}",
        f"- candidate_id: {summary.get('candidate_id')}",
        f"- failed_evidence_gates: {summary.get('failed_evidence_gate_count')}",
        f"- failed_stress_scenarios: {summary.get('failed_stress_scenario_count')}",
        f"- data_quality_issues: {summary.get('data_quality_issue_count')}",
        f"- safety_boundary_issues: {summary.get('safety_boundary_issue_count')}",
        f"- can_revisit: {summary.get('can_revisit')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Required Sections",
        "",
        "|section_id|title|required_fields|field_type|",
        "|---|---|---|---|",
    ]
    for section in _records(payload.get("required_sections")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    section.get("section_id"),
                    section.get("title"),
                    ", ".join(_list_values(section.get("required_fields"))),
                    section.get("field_type"),
                )
            )
            + "|"
        )
    lines.extend(
        [
            "",
            "## Filled Postmortem Validation",
            "",
            "|check_id|status|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for check in _records(_mapping(payload.get("filled_postmortem_validation")).get("checks")):
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
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def render_candidate_rejection_postmortem_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Candidate Rejection Postmortem Template Validation {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- filled_postmortem_status: {summary.get('filled_postmortem_status')}",
        f"- postmortem_record_provided: {summary.get('postmortem_record_provided')}",
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


def _source_context(
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
            "candidate_id": _candidate_id_from_payload(payload),
            "production_effect": _text(
                payload.get("production_effect"),
                _text(entry.get("production_effect"), PRODUCTION_EFFECT),
            ),
        }
    return context


def _read_context_payload(artifact_path: Path | None) -> tuple[Path | None, dict[str, Any]]:
    if artifact_path is None:
        return None, {}
    candidates = [
        artifact_path,
        artifact_path.with_suffix(".json"),
    ]
    seen: set[str] = set()
    for candidate in candidates:
        if str(candidate) in seen:
            continue
        seen.add(str(candidate))
        payload = _read_optional_json_mapping(candidate)
        if payload:
            return candidate, payload
    return None, {}


def _validate_filled_postmortem(record: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    if not record:
        return {
            "status": NO_POSTMORTEM_RECORD_PROVIDED,
            "check_count": 0,
            "failed_check_count": 0,
            "blocking_issues": [],
            "checks": [],
            "section_counts": _section_counts(record),
        }

    _append_check(
        checks,
        blocking_issues,
        "record_production_effect_none",
        _text(record.get("production_effect")) == PRODUCTION_EFFECT,
        "Filled postmortem must be production_effect=none.",
        "restore_postmortem_record_production_effect_none",
    )
    for section in REQUIRED_SECTIONS:
        section_id = _text(section.get("section_id"))
        value = record.get(section_id)
        expected_type = _text(section.get("field_type"))
        present = section_id in record
        valid_type = (
            isinstance(value, list) if expected_type == "list" else isinstance(value, dict)
        )
        _append_check(
            checks,
            blocking_issues,
            f"{section_id}_present_and_typed",
            present and valid_type,
            f"{section_id} must be present as {expected_type}.",
            f"fill_required_postmortem_section_{section_id}",
        )
        if not present or not valid_type:
            continue
        _check_section_required_fields(section, value, checks, blocking_issues)

    evidence_issue_count = sum(
        len(_records(record.get(section_id))) for section_id in LIST_SECTION_IDS[:4]
    )
    _append_check(
        checks,
        blocking_issues,
        "at_least_one_rejection_failure_recorded",
        evidence_issue_count > 0,
        (
            "A filled rejection postmortem must include at least one failed gate, "
            "scenario, data issue, or safety issue."
        ),
        "record_rejection_failure_evidence_before_archive",
    )
    revisit = _mapping(record.get("revisit_assessment"))
    _append_check(
        checks,
        blocking_issues,
        "revisit_decision_boolean",
        isinstance(revisit.get("can_revisit"), bool),
        "revisit_assessment.can_revisit must be a boolean.",
        "record_explicit_revisit_decision",
    )
    lessons = _records(record.get("lessons_learned"))
    _append_check(
        checks,
        blocking_issues,
        "lessons_learned_non_empty",
        bool(lessons),
        "lessons_learned must contain at least one lesson entry.",
        "record_lessons_learned_before_archive",
    )
    _append_check(
        checks,
        blocking_issues,
        "record_safety_boundary_no_mutation",
        _record_safety_flags_clear(record),
        "Filled postmortem must not contain production, broker, order, or state mutation flags.",
        "remove_or_correct_unsafe_postmortem_state_mutation_flags",
    )

    status = POSTMORTEM_INVALID if blocking_issues else POSTMORTEM_VALID
    return {
        "status": status,
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "blocking_issues": blocking_issues,
        "checks": checks,
        "section_counts": _section_counts(record),
    }


def _check_section_required_fields(
    section: Mapping[str, Any],
    value: Any,
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
) -> None:
    section_id = _text(section.get("section_id"))
    fields = _list_values(section.get("required_fields"))
    if isinstance(value, dict):
        missing = [field for field in fields if not _text(value.get(field))]
    else:
        missing = [
            f"{index}:{field}"
            for index, row in enumerate(_records(value), start=1)
            for field in fields
            if not _text(row.get(field))
        ]
    _append_check(
        checks,
        blocking_issues,
        f"{section_id}_required_fields",
        not missing,
        f"{section_id} must fill required fields: {', '.join(fields)}.",
        f"fill_required_fields_for_{section_id}",
        details={"missing_fields": missing},
    )


def _summary(
    context: Mapping[str, Mapping[str, Any]],
    postmortem_record: Mapping[str, Any],
    filled_validation: Mapping[str, Any],
) -> dict[str, Any]:
    record_provided = bool(postmortem_record)
    section_counts = _mapping(filled_validation.get("section_counts"))
    revisit = _mapping(postmortem_record.get("revisit_assessment"))
    return {
        "template_status": (
            TEMPLATE_BLOCKED
            if filled_validation.get("status") == POSTMORTEM_INVALID
            else TEMPLATE_READY
        ),
        "postmortem_record_provided": record_provided,
        "filled_postmortem_status": _text(
            filled_validation.get("status"),
            NO_POSTMORTEM_RECORD_PROVIDED,
        ),
        "candidate_id": _candidate_id_from_payload(postmortem_record)
        or _context_candidate_id(context),
        "required_section_count": len(REQUIRED_SECTION_IDS),
        "failed_evidence_gate_count": _int(section_counts.get("failed_evidence_gates")),
        "failed_stress_scenario_count": _int(section_counts.get("failed_stress_scenarios")),
        "data_quality_issue_count": _int(section_counts.get("data_quality_issues")),
        "safety_boundary_issue_count": _int(section_counts.get("safety_boundary_issues")),
        "lessons_learned_count": _int(section_counts.get("lessons_learned")),
        "can_revisit": revisit.get("can_revisit") if record_provided else "UNSPECIFIED",
        "promotion_board_status": _text(
            _mapping(context.get("paper_shadow_promotion_board")).get("status"),
            "MISSING",
        ),
        "owner_decision_status": _text(
            _mapping(context.get("owner_decision_audit_log")).get("status"),
            "MISSING",
        ),
        "safety_audit_status": _text(
            _mapping(context.get("research_safety_boundary_audit")).get("status"),
            "MISSING",
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _blank_postmortem_template(
    *,
    as_of: date,
    context: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_hint = _context_candidate_id(context)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "candidate_rejection_postmortem_record",
        "as_of": as_of.isoformat(),
        "production_effect": PRODUCTION_EFFECT,
        "candidate_summary": {
            "candidate_id": candidate_hint,
            "candidate_name": "",
            "rejection_decision_source": "",
            "market_regime": "ai_after_chatgpt",
            "source_artifacts": [],
        },
        "reason_for_rejection": {
            "primary_reason": "",
            "rejection_category": "",
            "decision_reference": "",
            "owner_action": "reject_candidate",
        },
        "failed_evidence_gates": [],
        "failed_stress_scenarios": [],
        "data_quality_issues": [],
        "safety_boundary_issues": [],
        "revisit_assessment": {
            "can_revisit": False,
            "revisit_condition": "",
            "owner_review_required": True,
        },
        "lessons_learned": [],
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
    }


def _required_section_contract() -> list[dict[str, Any]]:
    return [
        {
            "section_id": _text(section.get("section_id")),
            "title": _text(section.get("title")),
            "required_fields": _list_values(section.get("required_fields")),
            "field_type": _text(section.get("field_type")),
        }
        for section in REQUIRED_SECTIONS
    ]


def _reader_brief(summary: Mapping[str, Any]) -> dict[str, Any]:
    filled_status = _text(summary.get("filled_postmortem_status"))
    record_provided = summary.get("postmortem_record_provided") is True
    blocking = "none"
    if filled_status == POSTMORTEM_INVALID:
        blocking = "filled_postmortem_invalid"
    return {
        "summary": (
            f"Candidate rejection postmortem template is {_text(summary.get('template_status'))}; "
            f"filled postmortem status is {filled_status}."
        ),
        "key_result": _text(summary.get("template_status")),
        "blocking_issues": blocking,
        "warnings": (
            "no_filled_postmortem_record_provided"
            if not record_provided
            else "none"
        ),
        "safety_boundary": (
            "Manual research governance only; no candidate rejection is applied, "
            "no state is mutated, no official target weights, no broker/order, "
            "production_effect=none."
        ),
        "next_action": (
            "repair_filled_postmortem_before_archive"
            if filled_status == POSTMORTEM_INVALID
            else (
                "archive_owner_filled_rejection_postmortem"
                if record_provided
                else "use_template_if_owner_rejects_candidate"
            )
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _source_status(payload: Mapping[str, Any], entry: Mapping[str, Any]) -> str:
    for field in (
        "status",
        "template_status",
        "validation_status",
        "board_decision",
        "monthly_review_status",
        "safety_status",
        "audit_log_status",
    ):
        value = _text(payload.get(field), _text(_mapping(payload.get("summary")).get(field)))
        if value:
            return value
    for field in ("status", "freshness_status", "artifact_status"):
        value = _text(entry.get(field))
        if value:
            return value
    return "MISSING" if not entry else "UNKNOWN"


def _candidate_id_from_payload(payload: Mapping[str, Any]) -> str:
    for key in ("candidate_id", "candidate"):
        value = _text(payload.get(key))
        if value:
            return value
    for section_id in ("summary", "candidate_summary"):
        section = _mapping(payload.get(section_id))
        for key in ("candidate_id", "candidate"):
            value = _text(section.get(key))
            if value:
                return value
    return ""


def _context_candidate_id(context: Mapping[str, Mapping[str, Any]]) -> str:
    for item in context.values():
        candidate = _text(item.get("candidate_id"))
        if candidate:
            return candidate
    return ""


def _section_counts(record: Mapping[str, Any]) -> dict[str, int]:
    return {
        "failed_evidence_gates": len(_records(record.get("failed_evidence_gates"))),
        "failed_stress_scenarios": len(_records(record.get("failed_stress_scenarios"))),
        "data_quality_issues": len(_records(record.get("data_quality_issues"))),
        "safety_boundary_issues": len(_records(record.get("safety_boundary_issues"))),
        "lessons_learned": len(_records(record.get("lessons_learned"))),
    }


def _record_safety_flags_clear(record: Mapping[str, Any]) -> bool:
    unsafe_boolean_fields = (
        "candidate_state_mutated",
        "paper_shadow_state_mutated",
        "production_state_mutated",
        "official_target_weights_generated",
        "broker_action_taken",
        "order_ticket_generated",
    )
    return all(record.get(field) is False for field in unsafe_boolean_fields)


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "manual_research_governance_template",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_rejection": True,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "automatic_candidate_rejection": False,
        "live_trading_allowed": False,
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
