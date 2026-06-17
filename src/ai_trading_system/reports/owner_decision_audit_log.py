from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
AUDIT_RECORD_SCHEMA_VERSION = "owner_decision_audit_record_v1"
REPORT_TYPE = "owner_decision_audit_log"
VALIDATION_REPORT_TYPE = "owner_decision_audit_log_validation"
PRODUCTION_EFFECT = "none"

AUDIT_LOG_PASS = "AUDIT_LOG_PASS"
AUDIT_LOG_EMPTY = "AUDIT_LOG_EMPTY"
AUDIT_LOG_BLOCKED = "AUDIT_LOG_BLOCKED"
PASS_STATUS = "PASS"
FAIL_STATUS = "FAIL"

DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH = (
    PROJECT_ROOT / "data" / "governance" / "owner_decision_audit_log.jsonl"
)

OWNER_ACTIONS: tuple[str, ...] = (
    "continue_shadow",
    "enter_extended_shadow",
    "needs_more_evidence",
    "keep_hold",
    "approve_resume_normal_shadow",
    "return_to_research",
    "reject_candidate",
    "hold",
)
SAFETY_STATUSES: tuple[str, ...] = (
    "SAFETY_PASS",
    "SAFETY_PASS_WITH_WARNINGS",
    "SAFETY_BLOCKED",
)
REQUIRED_RECORD_FIELDS: tuple[str, ...] = (
    "decision_id",
    "timestamp",
    "candidate_id",
    "input_artifacts",
    "owner_action",
    "reason_summary",
    "safety_status",
    "next_action",
)
SAFE_FALSE_FIELDS: tuple[str, ...] = (
    "strategy_outputs_mutated",
    "candidate_state_mutated",
    "paper_shadow_state_mutated",
    "broker_action_taken",
    "order_ticket_generated",
    "official_target_weights_generated",
)


class OwnerDecisionAuditLogError(ValueError):
    """Raised when an owner decision audit record cannot be appended safely."""


def default_owner_decision_audit_log_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"owner_decision_audit_log_{as_of.isoformat()}.json"


def default_owner_decision_audit_log_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"owner_decision_audit_log_{as_of.isoformat()}.md"


def default_owner_decision_audit_log_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"owner_decision_audit_log_validation_{as_of.isoformat()}.json"


def default_owner_decision_audit_log_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"owner_decision_audit_log_validation_{as_of.isoformat()}.md"


def latest_owner_decision_audit_log_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "owner_decision_audit_log_", ".json")


def normalize_owner_decision_record(
    record: Mapping[str, Any],
    *,
    source_record_path: Path | None = None,
    appended_at: datetime | None = None,
) -> dict[str, Any]:
    appended = appended_at or datetime.now(tz=UTC)
    timestamp = _first_text(
        record,
        ("timestamp", "decision_timestamp", "owner_decision_timestamp", "created_at"),
        default=appended.isoformat(),
    )
    owner_action = _first_text(record, ("owner_action", "final_owner_action"))
    input_artifacts = _normalize_input_artifacts(
        record.get("input_artifacts", record.get("linked_input_artifacts"))
    )
    normalized: dict[str, Any] = {
        "record_schema_version": AUDIT_RECORD_SCHEMA_VERSION,
        "decision_id": _first_text(record, ("decision_id", "owner_decision_id")),
        "timestamp": timestamp,
        "candidate_id": _first_text(record, ("candidate_id", "candidate")),
        "input_artifacts": input_artifacts,
        "owner_action": owner_action,
        "reason_summary": _reason_summary(record),
        "safety_status": _first_text(record, ("safety_status", "latest_safety_status")),
        "next_action": _first_text(record, ("next_action", "required_follow_up")),
        "source_review_template_version": _first_text(
            record,
            ("template_version", "source_review_template_version"),
        ),
        "source_record_path": "" if source_record_path is None else str(source_record_path),
        "appended_at": appended.isoformat(),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "append_only_log_mutation": "append_one_jsonl_record",
        "strategy_outputs_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "official_target_weights_generated": False,
    }
    return normalized


def validate_owner_decision_record(
    record: Mapping[str, Any],
    *,
    line_number: int | None = None,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []

    for field_id in REQUIRED_RECORD_FIELDS:
        value = record.get(field_id)
        if field_id == "input_artifacts":
            artifacts = _records(value)
            passed = bool(artifacts) and all(_artifact_valid(artifact) for artifact in artifacts)
        else:
            passed = bool(_text(value))
        _append_check(
            checks,
            blocking_issues,
            check_id=f"required_field_{field_id}",
            passed=passed,
            message=f"Owner decision audit record must provide {field_id}.",
            recommended_action=f"fill_owner_decision_audit_field_{field_id}",
            line_number=line_number,
        )

    _append_check(
        checks,
        blocking_issues,
        check_id="record_schema_version",
        passed=_text(record.get("record_schema_version"), AUDIT_RECORD_SCHEMA_VERSION)
        == AUDIT_RECORD_SCHEMA_VERSION,
        message=f"record_schema_version must be {AUDIT_RECORD_SCHEMA_VERSION}.",
        recommended_action="regenerate_owner_decision_record_with_current_schema",
        line_number=line_number,
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="timestamp_iso8601",
        passed=_parse_datetime(_text(record.get("timestamp"))) is not None,
        message="timestamp must be ISO-8601 parseable.",
        recommended_action="record_owner_decision_timestamp_in_iso8601_utc",
        line_number=line_number,
    )
    owner_action = _text(record.get("owner_action"))
    _append_check(
        checks,
        blocking_issues,
        check_id="owner_action_enum",
        passed=owner_action in OWNER_ACTIONS,
        message="owner_action must use owner review template v2 action enum.",
        recommended_action="select_supported_owner_action",
        line_number=line_number,
    )
    safety_status = _text(record.get("safety_status"))
    _append_check(
        checks,
        blocking_issues,
        check_id="safety_status_enum",
        passed=safety_status in SAFETY_STATUSES,
        message="safety_status must use supported research safety statuses.",
        recommended_action="copy_latest_research_safety_boundary_status",
        line_number=line_number,
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="safety_blocked_cannot_continue_shadow",
        passed=not (
            safety_status == "SAFETY_BLOCKED"
            and owner_action
            in {
                "continue_shadow",
                "enter_extended_shadow",
                "approve_resume_normal_shadow",
            }
        ),
        message=(
            "SAFETY_BLOCKED cannot pair with continue_shadow, "
            "enter_extended_shadow, or approve_resume_normal_shadow."
        ),
        recommended_action="resolve_safety_blocker_or_choose_hold_return_research_reject",
        line_number=line_number,
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect_none",
        passed=_text(record.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT,
        message="Owner decision audit records must remain production_effect=none.",
        recommended_action="remove_production_effect_from_owner_decision_record",
        line_number=line_number,
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="manual_review_only",
        passed=record.get("manual_review_only", True) is True,
        message="Owner decision audit records must be manual_review_only=true.",
        recommended_action="restore_manual_review_only_boundary",
        line_number=line_number,
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="governance_only",
        passed=record.get("governance_only", True) is True,
        message="Owner decision audit records must be governance_only=true.",
        recommended_action="restore_governance_only_boundary",
        line_number=line_number,
    )
    for field_id in SAFE_FALSE_FIELDS:
        _append_check(
            checks,
            blocking_issues,
            check_id=f"{field_id}_false",
            passed=record.get(field_id, False) is False,
            message=f"{field_id} must remain false.",
            recommended_action=f"remove_{field_id}_mutation_from_audit_log",
            line_number=line_number,
        )

    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    return {
        "status": status,
        "decision_id": _text(record.get("decision_id")),
        "line_number": line_number,
        "check_count": len(checks),
        "failed_check_count": len(
            [check for check in checks if check["status"] == FAIL_STATUS]
        ),
        "checks": checks,
        "blocking_issues": blocking_issues,
    }


def append_owner_decision_record(
    record: Mapping[str, Any],
    *,
    log_path: Path = DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    source_record_path: Path | None = None,
    appended_at: datetime | None = None,
) -> dict[str, Any]:
    normalized = normalize_owner_decision_record(
        record,
        source_record_path=source_record_path,
        appended_at=appended_at,
    )
    validation = validate_owner_decision_record(normalized)
    if validation["status"] == FAIL_STATUS:
        raise OwnerDecisionAuditLogError(_issue_message(validation["blocking_issues"]))

    existing = read_owner_decision_audit_log(log_path)
    if existing["parse_issues"]:
        raise OwnerDecisionAuditLogError(
            f"owner decision audit log has malformed JSONL lines: {log_path}"
        )
    existing_ids = {
        _text(item["record"].get("decision_id"))
        for item in _records(existing.get("entries"))
        if _text(item["record"].get("decision_id"))
    }
    decision_id = _text(normalized.get("decision_id"))
    if decision_id in existing_ids:
        raise OwnerDecisionAuditLogError(f"duplicate owner decision_id: {decision_id}")

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(normalized, ensure_ascii=False, sort_keys=True) + "\n")
    return normalized


def read_owner_decision_audit_log(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "log_path": str(path),
            "log_exists": False,
            "entries": [],
            "parse_issues": [],
        }

    entries: list[dict[str, Any]] = []
    parse_issues: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            parse_issues.append(
                {
                    "issue_id": "blank_jsonl_line",
                    "line_number": line_number,
                    "message": "Owner decision audit log must not contain blank lines.",
                    "recommended_action": "remove_blank_line_by_recreating_log_from_source_control",
                }
            )
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            parse_issues.append(
                {
                    "issue_id": "malformed_jsonl_line",
                    "line_number": line_number,
                    "message": "Owner decision audit log line is not valid JSON.",
                    "recommended_action": "investigate_corrupt_audit_log_before_appending",
                }
            )
            continue
        if not isinstance(raw, dict):
            parse_issues.append(
                {
                    "issue_id": "jsonl_line_not_object",
                    "line_number": line_number,
                    "message": "Owner decision audit log line must be a JSON object.",
                    "recommended_action": "rewrite_source_decision_as_object_before_appending",
                }
            )
            continue
        entries.append({"line_number": line_number, "record": raw})
    return {
        "log_path": str(path),
        "log_exists": True,
        "entries": entries,
        "parse_issues": parse_issues,
    }


def build_owner_decision_audit_log_payload(
    *,
    as_of: date,
    log_path: Path = DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
) -> dict[str, Any]:
    read_result = read_owner_decision_audit_log(log_path)
    entries = _records(read_result.get("entries"))
    records = [_mapping(entry.get("record")) for entry in entries]
    parse_issues = _records(read_result.get("parse_issues"))
    record_validations = [
        validate_owner_decision_record(
            _mapping(entry.get("record")),
            line_number=_int(entry.get("line_number")),
        )
        for entry in entries
    ]
    invalid_validations = [
        validation
        for validation in record_validations
        if validation.get("status") == FAIL_STATUS
    ]
    duplicate_ids = _duplicate_decision_ids(records)
    duplicate_issues = [
        {
            "issue_id": "duplicate_decision_id",
            "decision_id": decision_id,
            "message": "Owner decision audit log decision_id values must be unique.",
            "recommended_action": "append_a_new_decision_id_or_investigate_duplicate_log_entry",
        }
        for decision_id in duplicate_ids
    ]
    future_records = [
        record
        for record in records
        if (record_date := _record_date(record)) is not None and record_date > as_of
    ]
    included_records = [
        record
        for record in records
        if (record_date := _record_date(record)) is None or record_date <= as_of
    ]
    latest_record = included_records[-1] if included_records else {}
    blocking_issues = (
        parse_issues
        + duplicate_issues
        + [
            issue
            for validation in invalid_validations
            for issue in _records(validation.get("blocking_issues"))
        ]
    )
    status = AUDIT_LOG_BLOCKED
    if not blocking_issues:
        status = AUDIT_LOG_PASS if included_records else AUDIT_LOG_EMPTY
    summary = {
        "log_path": str(log_path),
        "log_exists": read_result.get("log_exists") is True,
        "append_only_log": True,
        "record_count": len(records),
        "included_record_count": len(included_records),
        "records_after_as_of_count": len(future_records),
        "unique_decision_id_count": len(
            {
                _text(record.get("decision_id"))
                for record in records
                if _text(record.get("decision_id"))
            }
        ),
        "duplicate_decision_id_count": len(duplicate_ids),
        "invalid_record_count": len(invalid_validations),
        "parse_issue_count": len(parse_issues),
        "blocking_issue_count": len(blocking_issues),
        "candidate_count": len(
            {
                _text(record.get("candidate_id"))
                for record in included_records
                if _text(record.get("candidate_id"))
            }
        ),
        "owner_action_counts": dict(
            Counter(_text(record.get("owner_action")) for record in included_records)
        ),
        "safety_status_counts": dict(
            Counter(_text(record.get("safety_status")) for record in included_records)
        ),
        "latest_decision_id": _text(latest_record.get("decision_id")),
        "latest_timestamp": _text(latest_record.get("timestamp")),
        "latest_candidate_id": _text(latest_record.get("candidate_id")),
        "latest_owner_action": _text(latest_record.get("owner_action")),
        "latest_safety_status": _text(latest_record.get("safety_status")),
        "monthly_review_pack_input": (
            "AVAILABLE" if included_records else "NO_OWNER_DECISIONS_RECORDED"
        ),
        "promotion_board_input": (
            "AVAILABLE" if included_records else "NO_OWNER_DECISIONS_RECORDED"
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "audit_log_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "append_only_behavior": True,
        "purpose": (
            "Record owner decisions in an append-only governance audit log for "
            "monthly review and paper-shadow promotion board inputs."
        ),
        "input_artifacts": {
            "owner_decision_audit_log": str(log_path),
            "owner_decision_audit_log_exists": read_result.get("log_exists") is True,
        },
        "output_decision": status,
        "summary": summary,
        "audit_log_schema": _audit_log_schema(),
        "records": included_records,
        "records_after_as_of": future_records,
        "record_validations": record_validations,
        "blocking_issues": blocking_issues,
        "warning_issues": [],
        "monthly_review_pack_inputs": _downstream_input(
            "monthly_review_pack",
            status,
            summary,
        ),
        "promotion_board_inputs": _downstream_input(
            "paper_shadow_promotion_board",
            status,
            summary,
        ),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "The report reads the append-only audit log and does not append decisions.",
            "Owner decision audit entries are governance-only and do not mutate strategy outputs.",
            "Empty log status is allowed when no owner decision has been recorded yet.",
        ],
        "next_action": _next_action(status, len(included_records)),
        "reader_brief": _reader_brief(
            status,
            summary,
            PASS_STATUS if not blocking_issues else FAIL_STATUS,
        ),
        "methodology": {
            "collector_mode": "read_existing_append_only_jsonl_log",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_strategy_outputs": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_owner_decision_audit_log_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        message=f"report_type must be {REPORT_TYPE}.",
        recommended_action="regenerate_owner_decision_audit_log_report",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect_none",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        message="owner decision audit log report must be production_effect=none.",
        recommended_action="regenerate_report_without_production_effect",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="audit_log_not_blocked",
        passed=_text(payload.get("audit_log_status"), _text(payload.get("status")))
        != AUDIT_LOG_BLOCKED,
        message="owner decision audit log must not have blocking parse/schema issues.",
        recommended_action="fix_owner_decision_audit_log_before_downstream_review",
    )
    for field_id in (
        "parse_issue_count",
        "invalid_record_count",
        "duplicate_decision_id_count",
        "blocking_issue_count",
    ):
        _append_check(
            checks,
            blocking_issues,
            check_id=f"{field_id}_zero",
            passed=_int(summary.get(field_id)) == 0,
            message=f"{field_id} must be zero for validation PASS.",
            recommended_action="repair_owner_decision_audit_log_and_regenerate_report",
        )
    _append_check(
        checks,
        blocking_issues,
        check_id="required_schema_fields_declared",
        passed=set(REQUIRED_RECORD_FIELDS).issubset(
            set(_list_values(_mapping(payload.get("audit_log_schema")).get("required_fields")))
        ),
        message="audit_log_schema must declare every required owner decision field.",
        recommended_action="restore_owner_decision_audit_log_schema",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="append_only_behavior_declared",
        passed=payload.get("append_only_behavior") is True
        and summary.get("append_only_log") is True,
        message="owner decision audit log must declare append-only behavior.",
        recommended_action="restore_append_only_log_contract",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="monthly_review_pack_input_declared",
        passed=bool(_mapping(payload.get("monthly_review_pack_inputs")).get("input_status")),
        message="monthly_review_pack_inputs must expose owner decision log status.",
        recommended_action="restore_monthly_review_pack_inputs",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="promotion_board_input_declared",
        passed=bool(_mapping(payload.get("promotion_board_inputs")).get("input_status")),
        message="promotion_board_inputs must expose owner decision log status.",
        recommended_action="restore_promotion_board_inputs",
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        check_id="safety_boundary_no_strategy_mutation",
        passed=(
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("strategy_outputs_mutated") is False
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
        ),
        message=(
            "owner decision audit log must not mutate strategy, paper-shadow, broker, "
            "or order state."
        ),
        recommended_action="restore_governance_only_safety_boundary",
    )

    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "blocking_issue_count": len(blocking_issues),
        "source_record_count": _int(summary.get("record_count")),
        "included_record_count": _int(summary.get("included_record_count")),
        "empty_log_allowed": True,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "source_audit_log_status": _text(
            payload.get("audit_log_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "purpose": "Validate owner decision audit log report and append-only governance boundary.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": status,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": [],
        "source_summary": dict(summary),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation is read-only and does not append an owner decision.",
            "Validation allows an empty log when no owner decision has been recorded.",
        ],
        "next_action": (
            "use_owner_decision_audit_log_as_governance_input"
            if status == PASS_STATUS
            else "repair_owner_decision_audit_log_before_monthly_or_promotion_review"
        ),
        "reader_brief": _reader_brief(
            _text(payload.get("audit_log_status"), _text(payload.get("status"), "UNKNOWN")),
            summary,
            status,
        ),
        "methodology": {
            "collector_mode": "validate_existing_owner_decision_audit_report_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_append_decision": True,
            "does_not_modify_strategy_outputs": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_owner_decision_audit_log_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_owner_decision_audit_log_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_owner_decision_audit_log_markdown(payload), encoding="utf-8")
    return output_path


def write_owner_decision_audit_log_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_owner_decision_audit_log_json(payload, output_path)


def write_owner_decision_audit_log_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_owner_decision_audit_log_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_owner_decision_audit_log_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Owner Decision Audit Log {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- audit_log_status: {payload.get('audit_log_status')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- append_only_behavior: {payload.get('append_only_behavior')}",
        f"- record_count: {summary.get('record_count')}",
        f"- included_record_count: {summary.get('included_record_count')}",
        f"- blocking_issue_count: {summary.get('blocking_issue_count')}",
        f"- latest_decision_id: {summary.get('latest_decision_id')}",
        f"- latest_owner_action: {summary.get('latest_owner_action')}",
        f"- latest_safety_status: {summary.get('latest_safety_status')}",
        f"- monthly_review_pack_input: {summary.get('monthly_review_pack_input')}",
        f"- promotion_board_input: {summary.get('promotion_board_input')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Required Record Fields",
        "",
        ", ".join(_list_values(_mapping(payload.get("audit_log_schema")).get("required_fields"))),
        "",
        "## Owner Action Counts",
        "",
        "|owner_action|count|",
        "|---|---:|",
    ]
    for action, count in _mapping(summary.get("owner_action_counts")).items():
        lines.append(f"|{action}|{count}|")
    lines.extend(
        [
            "",
            "## Blocking Issues",
            "",
            "|issue_id|line_number|message|recommended_action|",
            "|---|---:|---|---|",
        ]
    )
    for issue in _records(payload.get("blocking_issues")):
        lines.append(
            f"|{_text(issue.get('issue_id'))}|{_text(issue.get('line_number'))}|"
            f"{_text(issue.get('message'))}|{_text(issue.get('recommended_action'))}|"
        )
    lines.append("")
    return "\n".join(lines)


def render_owner_decision_audit_log_validation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Owner Decision Audit Log Validation {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_audit_log_status: {payload.get('source_audit_log_status')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- source_record_count: {summary.get('source_record_count')}",
        f"- empty_log_allowed: {summary.get('empty_log_allowed')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Checks",
        "",
        "|check_id|status|message|recommended_action|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_text(check.get('check_id'))}|{_text(check.get('status'))}|"
            f"{_text(check.get('message'))}|{_text(check.get('recommended_action'))}|"
        )
    lines.append("")
    return "\n".join(lines)


def _audit_log_schema() -> dict[str, Any]:
    return {
        "schema_version": AUDIT_RECORD_SCHEMA_VERSION,
        "record_format": "jsonl",
        "append_only": True,
        "required_fields": list(REQUIRED_RECORD_FIELDS),
        "owner_actions": list(OWNER_ACTIONS),
        "safety_statuses": list(SAFETY_STATUSES),
        "safety_blocked_cannot_continue_shadow": True,
        "production_effect_required": PRODUCTION_EFFECT,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "append_only_log": True,
        "strategy_outputs_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "official_target_weights_generated": False,
        "data_refreshed": False,
        "upstream_commands_run": False,
    }


def _downstream_input(
    downstream_consumer: str,
    status: str,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    input_status = _text(summary.get("monthly_review_pack_input"))
    if downstream_consumer == "paper_shadow_promotion_board":
        input_status = _text(summary.get("promotion_board_input"))
    return {
        "consumer": downstream_consumer,
        "input_status": input_status,
        "audit_log_status": status,
        "record_count": _int(summary.get("included_record_count")),
        "latest_decision_id": _text(summary.get("latest_decision_id")),
        "latest_owner_action": _text(summary.get("latest_owner_action")),
        "latest_safety_status": _text(summary.get("latest_safety_status")),
        "production_effect": PRODUCTION_EFFECT,
        "strategy_outputs_mutated": False,
        "next_action": (
            "consume_latest_owner_decision_context"
            if input_status == "AVAILABLE"
            else "continue_without_owner_decision_until_one_is_recorded"
        ),
    }


def _reader_brief(
    status: str,
    summary: Mapping[str, Any],
    validation_status: str,
) -> dict[str, Any]:
    return {
        "summary": "Owner decision audit log is governance-only and append-only.",
        "key_result": status,
        "validation_status": validation_status,
        "blocking_issues": _int(summary.get("blocking_issue_count")),
        "warnings": _int(summary.get("records_after_as_of_count")),
        "safety_boundary": (
            "production_effect=none; no strategy, paper-shadow, broker, or order mutation."
        ),
        "next_action": _next_action(status, _int(summary.get("included_record_count"))),
    }


def _next_action(status: str, record_count: int) -> str:
    if status == AUDIT_LOG_BLOCKED:
        return "repair_owner_decision_audit_log_before_monthly_or_promotion_review"
    if record_count == 0:
        return "append_owner_decision_after_next_manual_owner_review"
    return "use_owner_decision_audit_log_as_monthly_and_promotion_board_input"


def _normalize_input_artifacts(value: Any) -> list[dict[str, str]]:
    if isinstance(value, Mapping):
        if any(key in value for key in ("artifact_id", "artifact_path", "path", "report_id")):
            candidates: list[Any] = [value]
        else:
            candidates = [
                {"artifact_id": str(key), "artifact_path": str(item)}
                for key, item in value.items()
            ]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        candidates = list(value)
    elif value is None:
        candidates = []
    else:
        candidates = [value]
    return [_normalize_artifact(item, index) for index, item in enumerate(candidates, start=1)]


def _normalize_artifact(item: Any, index: int) -> dict[str, str]:
    if isinstance(item, Mapping):
        artifact_id = _first_text(
            item,
            ("artifact_id", "report_id", "family", "id"),
            default=f"input_artifact_{index}",
        )
        artifact_path = _first_text(
            item,
            ("artifact_path", "path", "latest_artifact_path", "source_path"),
        )
        artifact_type = _first_text(
            item,
            ("artifact_type", "type"),
            default="report_or_validation",
        )
        return {
            "artifact_id": artifact_id,
            "artifact_path": artifact_path,
            "artifact_type": artifact_type,
        }
    return {
        "artifact_id": f"input_artifact_{index}",
        "artifact_path": _text(item),
        "artifact_type": "report_or_validation",
    }


def _artifact_valid(item: Mapping[str, Any]) -> bool:
    return bool(_text(item.get("artifact_id"))) and bool(_text(item.get("artifact_path")))


def _reason_summary(record: Mapping[str, Any]) -> str:
    direct = _first_text(record, ("reason_summary", "reason", "rationale"))
    if direct:
        return direct
    parts = [
        _text(record.get("evidence_interpretation")),
        _text(record.get("main_reason_to_continue")),
        _text(record.get("main_reason_to_reject")),
    ]
    return " | ".join(part for part in parts if part)


def _duplicate_decision_ids(records: Sequence[Mapping[str, Any]]) -> list[str]:
    counts = Counter(_text(record.get("decision_id")) for record in records)
    return sorted(decision_id for decision_id, count in counts.items() if decision_id and count > 1)


def _record_date(record: Mapping[str, Any]) -> date | None:
    timestamp = _parse_datetime(_text(record.get("timestamp")))
    if timestamp is None:
        return None
    return timestamp.date()


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return datetime.combine(date.fromisoformat(value), datetime.min.time(), tzinfo=UTC)
        except ValueError:
            return None


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
    line_number: int | None = None,
) -> None:
    status = PASS_STATUS if passed else FAIL_STATUS
    check: dict[str, Any] = {
        "check_id": check_id,
        "status": status,
        "message": message,
        "recommended_action": recommended_action,
    }
    if line_number is not None:
        check["line_number"] = line_number
    checks.append(check)
    if not passed:
        issue = {
            "issue_id": check_id,
            "message": message,
            "recommended_action": recommended_action,
        }
        if line_number is not None:
            issue["line_number"] = line_number
        blocking_issues.append(issue)


def _issue_message(issues: Sequence[Mapping[str, Any]]) -> str:
    issue_ids = [_text(issue.get("issue_id")) for issue in issues[:3]]
    return "owner decision audit record failed validation: " + ", ".join(issue_ids)


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{4}}-\d{{2}}-\d{{2}}){re.escape(suffix)}$")
    candidates: list[tuple[date, Path]] = []
    if not output_dir.exists():
        return None
    for path in output_dir.iterdir():
        match = pattern.match(path.name)
        if not match:
            continue
        try:
            candidates.append((date.fromisoformat(match.group(1)), path))
        except ValueError:
            continue
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _first_text(
    mapping: Mapping[str, Any],
    keys: Sequence[str],
    *,
    default: str = "",
) -> str:
    for key in keys:
        text = _text(mapping.get(key))
        if text:
            return text
    return default


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _list_values(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
