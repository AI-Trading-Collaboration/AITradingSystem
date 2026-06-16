from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
REPORT_TYPE = "owner_review_template_v2"
VALIDATION_REPORT_TYPE = "owner_review_template_v2_validation"
PRODUCTION_EFFECT = "none"

TEMPLATE_READY = "TEMPLATE_READY"
TEMPLATE_BLOCKED = "TEMPLATE_BLOCKED"
PASS_STATUS = "PASS"
FAIL_STATUS = "FAIL"

OWNER_ACTIONS: tuple[str, ...] = (
    "continue_shadow",
    "enter_extended_shadow",
    "needs_more_evidence",
    "return_to_research",
    "reject_candidate",
    "hold",
)
SAFETY_STATUSES: tuple[str, ...] = (
    "SAFETY_PASS",
    "SAFETY_PASS_WITH_WARNINGS",
    "SAFETY_BLOCKED",
)
REQUIRED_FIELDS: tuple[dict[str, str], ...] = (
    {
        "field_id": "candidate_id",
        "type": "string",
        "description": "Stable candidate identifier under review.",
    },
    {
        "field_id": "evidence_interpretation",
        "type": "string",
        "description": "Owner interpretation of available evidence, including limits.",
    },
    {
        "field_id": "main_reason_to_continue",
        "type": "string",
        "description": "Primary evidence supporting continued observation or extended shadow.",
    },
    {
        "field_id": "main_reason_to_reject",
        "type": "string",
        "description": "Primary evidence supporting rejection or return to research.",
    },
    {
        "field_id": "uncertainty",
        "type": "string",
        "description": "Known uncertainty, ambiguity, missing data, or judgment risk.",
    },
    {
        "field_id": "required_follow_up",
        "type": "string",
        "description": "Specific follow-up required before the next review.",
    },
    {
        "field_id": "final_owner_action",
        "type": "enum",
        "description": "Final owner action from the v2 action enum.",
    },
    {
        "field_id": "linked_input_artifacts",
        "type": "array",
        "description": "One or more report, validation, or evidence artifacts used by the owner.",
    },
    {
        "field_id": "safety_status",
        "type": "enum",
        "description": "Latest safety boundary status used during the review.",
    },
)
REQUIRED_FIELD_IDS = tuple(field["field_id"] for field in REQUIRED_FIELDS)


def default_owner_review_template_v2_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"owner_review_template_v2_{as_of.isoformat()}.json"


def default_owner_review_template_v2_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"owner_review_template_v2_{as_of.isoformat()}.md"


def default_owner_review_template_v2_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"owner_review_template_v2_validation_{as_of.isoformat()}.json"


def default_owner_review_template_v2_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"owner_review_template_v2_validation_{as_of.isoformat()}.md"


def latest_owner_review_template_v2_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "owner_review_template_v2_", ".json")


def build_owner_review_template_v2_payload(*, as_of: date) -> dict[str, Any]:
    summary = {
        "required_field_count": len(REQUIRED_FIELDS),
        "owner_action_count": len(OWNER_ACTIONS),
        "safety_status_count": len(SAFETY_STATUSES),
        "optional_record_validation_supported": True,
        "manual_review_only": True,
        "owner_decision_logged": False,
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": TEMPLATE_READY,
        "template_status": TEMPLATE_READY,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "owner_decision_logged": False,
        "purpose": (
            "Define a reusable owner review template v2 so manual candidate reviews "
            "use consistent fields, comparable owner actions, linked input artifacts, "
            "and explicit safety status."
        ),
        "input_artifacts": {},
        "output_decision": TEMPLATE_READY,
        "summary": summary,
        "required_fields": [dict(field) for field in REQUIRED_FIELDS],
        "owner_action_options": _owner_action_options(),
        "safety_status_options": [
            {
                "safety_status": status,
                "description": _safety_status_description(status),
            }
            for status in SAFETY_STATUSES
        ],
        "blank_review_template": _blank_review_template(),
        "filled_review_validation_contract": {
            "required_fields": list(REQUIRED_FIELD_IDS),
            "owner_actions": list(OWNER_ACTIONS),
            "safety_statuses": list(SAFETY_STATUSES),
            "linked_input_artifacts_min_count": 1,
            "safety_blocked_cannot_continue_shadow": True,
            "production_effect_required": PRODUCTION_EFFECT,
        },
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "The template report does not create or append an owner decision.",
            "The template report does not run upstream evidence collection.",
            "Filled review validation only checks contract completeness and safety invariants.",
        ],
        "next_action": "use_template_for_next_manual_owner_review_or_validate_filled_review_json",
        "reader_brief": _reader_brief(TEMPLATE_READY, summary, PASS_STATUS),
        "source_artifacts": [],
        "methodology": {
            "collector_mode": "static_template_contract_generation_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_log_owner_decision": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }
    return payload


def validate_owner_review_template_v2_payload(
    payload: Mapping[str, Any],
    *,
    review_record: Mapping[str, Any] | None = None,
    review_record_path: Path | None = None,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []

    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        message=f"report_type must be {REPORT_TYPE}.",
        recommended_action="regenerate_owner_review_template_v2_report",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        message="owner review template v2 report must be production_effect=none.",
        recommended_action="regenerate_template_without_production_effect",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="template_status",
        passed=_text(payload.get("template_status"), _text(payload.get("status")))
        == TEMPLATE_READY,
        message="template_status must be TEMPLATE_READY.",
        recommended_action="repair_template_contract_before_use",
    )
    field_ids = _required_field_ids(payload.get("required_fields"))
    _append_check(
        checks,
        blocking_issues,
        check_id="required_fields_present",
        passed=set(REQUIRED_FIELD_IDS).issubset(field_ids),
        message="owner review template must declare every required v2 field.",
        recommended_action="restore_missing_required_template_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="owner_action_enum_exact",
        passed=_option_ids(payload.get("owner_action_options"), "owner_action")
        == set(OWNER_ACTIONS),
        message="owner action enum must match TRADING-364 exactly.",
        recommended_action="restore_owner_action_enum",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="safety_status_enum_exact",
        passed=_option_ids(payload.get("safety_status_options"), "safety_status")
        == set(SAFETY_STATUSES),
        message="safety status enum must match the research safety boundary statuses.",
        recommended_action="restore_safety_status_enum",
    )
    blank_template = _mapping(payload.get("blank_review_template"))
    _append_check(
        checks,
        blocking_issues,
        check_id="blank_template_has_required_keys",
        passed=set(REQUIRED_FIELD_IDS).issubset(set(blank_template)),
        message="blank_review_template must include every required v2 field key.",
        recommended_action="restore_blank_review_template_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="manual_review_only",
        passed=payload.get("manual_review_only") is True,
        message="owner review template must remain manual_review_only=true.",
        recommended_action="restore_manual_review_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="owner_decision_not_logged",
        passed=payload.get("owner_decision_logged") is False,
        message="TRADING-364 template must not log or append owner decisions.",
        recommended_action="move_append_only_decision_behavior_to_TRADING_378",
    )

    record_validation = _empty_record_validation()
    if review_record is not None:
        record_validation = validate_filled_owner_review_record(
            review_record,
            review_record_path=review_record_path,
        )
        checks.extend(_records(record_validation.get("checks")))
        blocking_issues.extend(_records(record_validation.get("blocking_issues")))

    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    summary = {
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "blocking_issue_count": len(blocking_issues),
        "warning_check_count": 0,
        "review_record_provided": review_record is not None,
        "review_record_check_count": _int(record_validation.get("check_count")),
        "review_record_failed_check_count": _int(record_validation.get("failed_check_count")),
        "required_field_count": len(REQUIRED_FIELDS),
        "owner_action_count": len(OWNER_ACTIONS),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "source_template_status": _text(
            payload.get("template_status"),
            _text(payload.get("status")),
        ),
        "production_effect": PRODUCTION_EFFECT,
        "purpose": "Validate owner review template v2 contract and optional filled review record.",
        "input_artifacts": {},
        "output_decision": status,
        "summary": summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": [],
        "review_record_validation": record_validation,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation is read-only and does not append a decision log entry.",
            "Optional filled review validation checks required fields and safety invariants only.",
        ],
        "next_action": _validation_next_action(status, review_record is not None),
        "reader_brief": _reader_brief(
            _text(payload.get("template_status"), TEMPLATE_BLOCKED),
            _mapping(payload.get("summary")),
            status,
        ),
        "methodology": {
            "collector_mode": "read_existing_template_and_optional_review_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_log_owner_decision": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_filled_owner_review_record(
    record: Mapping[str, Any],
    *,
    review_record_path: Path | None = None,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    for field_id in REQUIRED_FIELD_IDS:
        value = record.get(field_id)
        if field_id == "linked_input_artifacts":
            passed = bool(_list_values(value)) and all(
                _linked_artifact_valid(item) for item in _list_values(value)
            )
        else:
            passed = bool(_text(value))
        _append_check(
            checks,
            blocking_issues,
            check_id=f"filled_review_required_field_{field_id}",
            passed=passed,
            message=f"Filled owner review must provide {field_id}.",
            recommended_action=f"fill_owner_review_field_{field_id}",
        )

    owner_action = _text(record.get("final_owner_action"))
    _append_check(
        checks,
        blocking_issues,
        check_id="filled_review_owner_action_enum",
        passed=owner_action in OWNER_ACTIONS,
        message="Filled owner review final_owner_action must use owner review v2 enum.",
        recommended_action="select_supported_owner_action",
    )
    safety_status = _text(record.get("safety_status"))
    _append_check(
        checks,
        blocking_issues,
        check_id="filled_review_safety_status_enum",
        passed=safety_status in SAFETY_STATUSES,
        message="Filled owner review safety_status must use supported safety statuses.",
        recommended_action="copy_latest_research_safety_boundary_status",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="filled_review_production_effect_none",
        passed=_text(record.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT,
        message="Filled owner review must not declare production_effect other than none.",
        recommended_action="remove_production_effect_from_owner_review",
    )
    # Safety invariant: a safety-blocked review cannot recommend continued shadow observation.
    _append_check(
        checks,
        blocking_issues,
        check_id="safety_blocked_cannot_continue_shadow",
        passed=not (
            safety_status == "SAFETY_BLOCKED"
            and owner_action in {"continue_shadow", "enter_extended_shadow"}
        ),
        message="SAFETY_BLOCKED cannot pair with continue_shadow or enter_extended_shadow.",
        recommended_action="resolve_safety_blocker_or_choose_hold_return_research_reject",
    )

    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    return {
        "provided": True,
        "status": status,
        "review_record_path": "" if review_record_path is None else str(review_record_path),
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "checks": checks,
        "blocking_issues": blocking_issues,
    }


def write_owner_review_template_v2_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_owner_review_template_v2_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_owner_review_template_v2_markdown(payload), encoding="utf-8")
    return output_path


def write_owner_review_template_v2_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_owner_review_template_v2_json(payload, output_path)


def write_owner_review_template_v2_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_owner_review_template_v2_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_owner_review_template_v2_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Owner Review Template V2 {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- template_status: {payload.get('template_status')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- required_field_count: {summary.get('required_field_count')}",
        f"- owner_action_count: {summary.get('owner_action_count')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Required Fields",
        "",
        "|field_id|type|description|",
        "|---|---|---|",
    ]
    for field in _records(payload.get("required_fields")):
        lines.append(
            f"|{_text(field.get('field_id'))}|{_text(field.get('type'))}|"
            f"{_text(field.get('description'))}|"
        )
    lines.extend(
        [
            "",
            "## Owner Actions",
            "",
            "|owner_action|description|",
            "|---|---|",
        ]
    )
    for option in _records(payload.get("owner_action_options")):
        lines.append(
            f"|{_text(option.get('owner_action'))}|{_text(option.get('description'))}|"
        )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
        ]
    )
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Blank Review Template",
            "",
            "```json",
            json.dumps(payload.get("blank_review_template", {}), ensure_ascii=False, indent=2),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def render_owner_review_template_v2_validation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Owner Review Template V2 Validation {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- review_record_provided: {summary.get('review_record_provided')}",
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


def _blank_review_template() -> dict[str, Any]:
    return {
        "template_version": REPORT_TYPE,
        "candidate_id": "<candidate_id>",
        "evidence_interpretation": "<owner interpretation of evidence and limitations>",
        "main_reason_to_continue": "<primary continue evidence or none>",
        "main_reason_to_reject": "<primary reject evidence or none>",
        "uncertainty": "<known uncertainty or missing evidence>",
        "required_follow_up": "<required follow-up before next review>",
        "final_owner_action": "hold",
        "linked_input_artifacts": [
            {
                "artifact_id": "<artifact_id>",
                "artifact_path": "<path/to/input_artifact.json>",
                "artifact_type": "report_or_validation",
            }
        ],
        "safety_status": "SAFETY_PASS_WITH_WARNINGS",
        "production_effect": PRODUCTION_EFFECT,
    }


def _owner_action_options() -> list[dict[str, str]]:
    descriptions = {
        "continue_shadow": "Continue the current normal paper-shadow observation.",
        "enter_extended_shadow": (
            "Move to extended paper-shadow observation after governance checks."
        ),
        "needs_more_evidence": "Hold decision until specific missing evidence is collected.",
        "return_to_research": "Return candidate to research for redesign or more analysis.",
        "reject_candidate": "Reject candidate unless materially new evidence appears.",
        "hold": "Hold current state without continuation, extension, rejection, or redesign.",
    }
    return [
        {"owner_action": action, "description": descriptions[action]} for action in OWNER_ACTIONS
    ]


def _safety_status_description(status: str) -> str:
    return {
        "SAFETY_PASS": "Latest safety boundary check has no blocking or warning safety issue.",
        "SAFETY_PASS_WITH_WARNINGS": "Safety boundary is usable with visible warning review.",
        "SAFETY_BLOCKED": "Safety boundary blocks continuation until resolved.",
    }[status]


def _safety_boundary() -> dict[str, Any]:
    return {
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "owner_decision_logged": False,
        "append_only_decision_log_mutated": False,
        "upstream_evidence_collection_run": False,
        "data_refreshed": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "official_target_weights_generated": False,
        "order_ticket_generated": False,
        "broker_action_taken": False,
    }


def _reader_brief(
    template_status: str,
    summary: Mapping[str, Any],
    validation_status: str,
) -> dict[str, Any]:
    return {
        "summary": "Owner review template v2 provides a consistent manual review contract.",
        "key_result": template_status,
        "blocking_issues": "none" if validation_status == PASS_STATUS else "validation_failed",
        "warnings": "none",
        "safety_boundary": "production_effect=none; manual-review-only; no decision log mutation.",
        "next_action": "use_template_for_next_owner_review_and_validate_filled_review",
        "required_field_count": _int(summary.get("required_field_count")),
        "owner_action_count": _int(summary.get("owner_action_count")),
        "validation_status": validation_status,
    }


def _validation_next_action(status: str, review_record_provided: bool) -> str:
    if status == FAIL_STATUS:
        return "fix_owner_review_template_or_filled_review_before_use"
    if review_record_provided:
        return "record_review_only_through_owner_decision_audit_log_when_available"
    return "use_template_for_next_manual_owner_review"


def _empty_record_validation() -> dict[str, Any]:
    return {
        "provided": False,
        "status": "NOT_PROVIDED",
        "review_record_path": "",
        "check_count": 0,
        "failed_check_count": 0,
        "checks": [],
        "blocking_issues": [],
    }


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    *,
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
) -> None:
    status = PASS_STATUS if passed else FAIL_STATUS
    check = {
        "check_id": check_id,
        "status": status,
        "severity": "BLOCKING",
        "message": message,
        "recommended_action": "" if passed else recommended_action,
    }
    checks.append(check)
    if not passed:
        blocking_issues.append(
            {
                "issue_id": check_id,
                "severity": "BLOCKING",
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _linked_artifact_valid(item: Any) -> bool:
    if isinstance(item, str):
        return bool(item.strip())
    if isinstance(item, Mapping):
        return bool(
            _text(item.get("artifact_id"))
            or _text(item.get("artifact_path"))
            or _text(item.get("path"))
        )
    return False


def _required_field_ids(raw: Any) -> set[str]:
    return {
        _text(field.get("field_id"))
        for field in _records(raw)
        if _text(field.get("field_id"))
    }


def _option_ids(raw: Any, key: str) -> set[str]:
    return {_text(item.get(key)) for item in _records(raw) if _text(item.get(key))}


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    dated_name = re.compile(rf"^{re.escape(prefix)}\d{{4}}-\d{{2}}-\d{{2}}{re.escape(suffix)}$")
    candidates = sorted(
        path for path in output_dir.glob(f"{prefix}*{suffix}") if dated_name.match(path.name)
    )
    return candidates[-1] if candidates else None


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def _records(raw: Any) -> list[Mapping[str, Any]]:
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, Mapping)]


def _list_values(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if raw in (None, ""):
        return []
    return [raw]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
